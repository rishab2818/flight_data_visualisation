import os, json
from datetime import datetime
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from db.session import SessionLocal
from models import Dataset, Job, JobStatus
import packet_parser

# --- realtime bus (optional) ---
try:
    from events import events
except Exception:
    events = None


# ----------------- helpers -----------------

def compute_all_columns(schema_path: str) -> List[str]:
    with open(schema_path, "r") as f:
        schemas = json.load(f)

    cols = set(["PacketNum", "ID"])
    for schema in schemas:
        cols.update(schema.get("all_fields", []))
        for field in schema.get("fields", []):
            cols.add(field["name"])
            for bit in field.get("bits", []):
                cols.add(bit)
    return ["PacketNum", "ID"] + sorted(c for c in cols if c not in ("PacketNum", "ID"))


def _publish(job_id: str, payload: Dict):
    if events is not None:
        try:
            events.publish(job_id, payload)
        except Exception:
            pass


def append_log(job_id: str, text: str, *, progress: Optional[float] = None, message: Optional[str] = None):
    ts = datetime.utcnow().isoformat()
    payload = {"job_id": job_id, "type": "log", "ts": ts, "log": text}
    if progress is not None:
        payload["progress"] = progress
    if message is not None:
        payload["message"] = message
    _publish(job_id, payload)


def set_status(job_id: str, status: JobStatus, *, progress: Optional[float] = None, message: Optional[str] = None):
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id == job_id).first()
        if not j:
            return
        j.status = status
        if progress is not None:
            j.progress = progress
        if message is not None:
            j.message = message
        if status in (JobStatus.success, JobStatus.failed) and j.finished_at is None:
            j.finished_at = datetime.utcnow()
        db.add(j)
        db.commit()
    finally:
        db.close()

    _publish(job_id, {
        "job_id": job_id,
        "type": "status",
        "status": str(status),
        "progress": progress,
        "message": message,
    })


# ----------- robust frame scanning (stateful across chunks) -----------

class FrameScanner:
    """
    Stateful scanner: feed() with arbitrary bytes; yields complete frames [START ... END].
    Keeps buffer across chunk boundaries to avoid truncated/invalid packets.
    """
    def __init__(self, start_byte: int, end_byte: int):
        self.START = start_byte
        self.END = end_byte
        self._buf = bytearray()
        self._inside = False

    def feed(self, data: bytes):
        for b in data:
            if not self._inside:
                if b == self.START:
                    self._buf.clear()
                    self._buf.append(b)
                    self._inside = True
                # else skip until START appears
                continue
            else:
                self._buf.append(b)
                if b == self.END:
                    # complete frame
                    yield bytes(self._buf)
                    self._buf.clear()
                    self._inside = False
                elif b == self.START:
                    # resync on unexpected START inside a frame (drop previous partial)
                    self._buf.clear()
                    self._buf.append(b)
                    self._inside = True

    def flush(self):
        """Call at EOF to drop any partial frame."""
        self._buf.clear()
        self._inside = False


def _looks_like_hex_text(header: bytes) -> bool:
    # If the file is a hex-dump (e.g., "01 02 05 ..."), it contains only hex chars + whitespace.
    sample = header[:4096]
    allowed = set(b"0123456789abcdefABCDEF \t\r\n")
    # Heuristic: mostly allowed chars and not many NULs
    if not sample:
        return False
    bad = sum(1 for ch in sample if ch not in allowed)
    nul = sample.count(b"\x00")
    return bad == 0 and nul == 0


def _safe_create_packet(factory, frame: bytes):
    """
    Some factories expect list[int], others bytes; try both.
    """
    pkt = None
    try:
        pkt = factory.create_packet(list(frame))
    except Exception:
        try:
            pkt = factory.create_packet(frame)
        except Exception:
            pkt = None
    return pkt


# ----------------- main parse -----------------

def stream_to_parquet(job_id: str, dataset_id: str, raw_path: str, schema_file: str, batch_size: int = 1000):
    db = SessionLocal()
    try:
        set_status(job_id, JobStatus.running, progress=0.0, message="starting")
        append_log(job_id, "Parsing started", progress=0.0, message="starting")

        out_dir = os.path.dirname(raw_path)
        parquet_path = os.path.join(out_dir, "data.parquet")

        cols = compute_all_columns(schema_file)
        factory = packet_parser.PacketFactory(schema_file)
        START = getattr(packet_parser.Packet, "START_FRAME", 0x01)
        END = getattr(packet_parser.Packet, "END_FRAME", 0x05)
        scanner = FrameScanner(START, END)

        packet_num = 0
        valid = 0
        batch: List[Dict] = []
        writer: Optional[pq.ParquetWriter] = None

        # file size only for coarse progress (optional)
        try:
            file_size = os.path.getsize(raw_path)
        except OSError:
            file_size = None
        bytes_read = 0

        def pkt_to_row(pkt: "packet_parser.Packet") -> Dict:
            row = {c: None for c in cols}
            row["PacketNum"] = packet_num
            # let your Packet/Factory set ID in pkt.data if applicable
            row["ID"] = pkt.data.get("ID") if hasattr(pkt, "data") else None
            if hasattr(pkt, "data"):
                for k, v in pkt.data.items():
                    if k in row:
                        row[k] = v
            return row

        with open(raw_path, "rb") as f:
            # Peek header to detect hex-dump vs binary
            header = f.read(8192)
            bytes_read += len(header)
            hex_mode = _looks_like_hex_text(header)

            if hex_mode:
                # Process the peeked header lines first, then continue line by line.
                def _feed_hex_lines(chunk: bytes):
                    # split on lines; parse each as hex tokens -> bytes; feed to scanner
                    for line in chunk.splitlines():
                        s = line.strip()
                        if not s:
                            continue
                        try:
                            tokens = s.split()
                            data = bytes(int(tok, 16) for tok in tokens)
                        except Exception:
                            append_log(job_id, f"Malformed hex line (ignored)")
                            continue
                        for frame in scanner.feed(data):
                            yield frame

                # handle header
                for frame in _feed_hex_lines(header):
                    pkt = _safe_create_packet(factory, frame)
                    if not pkt:
                        append_log(job_id, "No schema for frame (hex mode)")
                        continue
                    try:
                        # Many repos require .parse() to populate pkt.data
                        if hasattr(pkt, "parse"):
                            pkt.parse()
                    except Exception as e:
                        append_log(job_id, f"Parse error: {e!r}")
                        continue

                    packet_num += 1
                    batch.append(pkt_to_row(pkt))
                    valid += 1
                    if len(batch) >= batch_size:
                        table = pa.Table.from_pylist(batch)
                        if writer is None:
                            writer = pq.ParquetWriter(parquet_path, table.schema)
                        writer.write_table(table)
                        batch.clear()
                        pct = None
                        if file_size:
                            pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                        append_log(job_id, f"Wrote {valid} packets", progress=pct, message="parsing")
                        set_status(job_id, JobStatus.running, progress=pct, message="parsing")

                # continue reading line by line
                for line in f:
                    bytes_read += len(line)
                    for frame in _feed_hex_lines(line):
                        pkt = _safe_create_packet(factory, frame)
                        if not pkt:
                            append_log(job_id, "No schema for frame (hex mode)")
                            continue
                        try:
                            if hasattr(pkt, "parse"):
                                pkt.parse()
                        except Exception as e:
                            append_log(job_id, f"Parse error: {e!r}")
                            continue

                        packet_num += 1
                        batch.append(pkt_to_row(pkt))
                        valid += 1
                        if len(batch) >= batch_size:
                            table = pa.Table.from_pylist(batch)
                            if writer is None:
                                writer = pq.ParquetWriter(parquet_path, table.schema)
                            writer.write_table(table)
                            batch.clear()
                            pct = None
                            if file_size:
                                pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                            append_log(job_id, f"Wrote {valid} packets", progress=pct, message="parsing")
                            set_status(job_id, JobStatus.running, progress=pct, message="parsing")

            else:
                # BINARY MODE: feed raw bytes to scanner; DO NOT decode
                if header:
                    for frame in scanner.feed(header):
                        pkt = _safe_create_packet(factory, frame)
                        if not pkt:
                            append_log(job_id, "No schema for frame (binary head)")
                            continue
                        try:
                            if hasattr(pkt, "parse"):
                                pkt.parse()
                        except Exception as e:
                            append_log(job_id, f"Parse error: {e!r}")
                            continue

                        packet_num += 1
                        batch.append(pkt_to_row(pkt))
                        valid += 1
                        if len(batch) >= batch_size:
                            table = pa.Table.from_pylist(batch)
                            if writer is None:
                                writer = pq.ParquetWriter(parquet_path, table.schema)
                            writer.write_table(table)
                            batch.clear()

                while True:
                    chunk = f.read(4 * 1024 * 1024)  # 4MB
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    for frame in scanner.feed(chunk):
                        pkt = _safe_create_packet(factory, frame)
                        if not pkt:
                            append_log(job_id, "No schema for frame (binary)")
                            continue
                        try:
                            if hasattr(pkt, "parse"):
                                pkt.parse()
                        except Exception as e:
                            append_log(job_id, f"Parse error: {e!r}")
                            continue

                        packet_num += 1
                        batch.append(pkt_to_row(pkt))
                        valid += 1
                        if len(batch) >= batch_size:
                            table = pa.Table.from_pylist(batch)
                            if writer is None:
                                writer = pq.ParquetWriter(parquet_path, table.schema)
                            writer.write_table(table)
                            batch.clear()

                    # sparse status checkpoint
                    pct = None
                    if file_size:
                        pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                    set_status(job_id, JobStatus.running, progress=pct, message="parsing")

            # drop any partial frame at EOF (don’t emit)
            scanner.flush()

        # flush remaining rows
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
            batch.clear()
        if writer:
            writer.close()

        # persist dataset metadata
        d = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if d:
            d.parquet_path = parquet_path
            d.columns_json = json.dumps(cols)
            d.packet_count = valid
            db.add(d)
            db.commit()

        append_log(job_id, f"Completed: rows={valid}", progress=95.0, message="parsed")
        set_status(job_id, JobStatus.success, progress=100.0, message="completed")

    except Exception as e:
        append_log(job_id, f"Error: {e!r}", progress=100.0, message="failed")
        set_status(job_id, JobStatus.failed, progress=100.0, message=str(e))
    finally:
        db.close()
