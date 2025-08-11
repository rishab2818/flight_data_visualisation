import os
import json
from datetime import datetime
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from db.session import SessionLocal
from models import Dataset, Job, JobStatus

# Optional realtime bus (OK if missing)
try:
    from events import events
except Exception:
    events = None


# ========== helpers ==========

START_FRAME = 0x01
END_FRAME = 0x05

def compute_all_columns(schema_path: str) -> List[str]:
    """
    Build superset of columns from packet_schema.json.
    """
    with open(schema_path, "r") as f:
        schemas = json.load(f)

    cols = {"PacketNum", "ID"}
    for s in schemas:
        cols.update(s.get("all_fields", []))
        for field in s.get("fields", []):
            cols.add(field["name"])
            for bit in field.get("bits", []):
                cols.add(bit)

    ordered = ["PacketNum", "ID"]
    ordered += sorted(c for c in cols if c not in ("PacketNum", "ID"))
    return ordered


def _publish(job_id: str, payload: Dict):
    if events is not None:
        try:
            events.publish(job_id, payload)
        except Exception:
            pass


def append_log(job_id: str, text: str, *, progress: Optional[float] = None, message: Optional[str] = None):
    """
    Do NOT persist logs to DB. Only push to frontend.
    """
    payload = {
        "job_id": job_id,
        "type": "log",
        "ts": datetime.utcnow().isoformat(),
        "log": text,
    }
    if progress is not None:
        payload["progress"] = progress
    if message is not None:
        payload["message"] = message
    _publish(job_id, payload)


def set_status(job_id: str, status: JobStatus, *, progress: Optional[float] = None, message: Optional[str] = None):
    """
    Persist only status/progress/message in DB; push realtime event.
    """
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


# ========== robust frame scanning (stateful) ==========

class FrameScanner:
    """
    Keeps state across chunks. Yields full frames [START ... END].
    """
    def __init__(self, start_b: int, end_b: int):
        self.START = start_b
        self.END = end_b
        self._buf = bytearray()
        self._inside = False

    def feed(self, data: bytes):
        for b in data:
            if not self._inside:
                if b == self.START:
                    self._buf.clear()
                    self._buf.append(b)
                    self._inside = True
                continue
            else:
                self._buf.append(b)
                if b == self.END:
                    yield bytes(self._buf)
                    self._buf.clear()
                    self._inside = False
                elif b == self.START:
                    # resync to a new start
                    self._buf.clear()
                    self._buf.append(b)
                    self._inside = True

    def flush(self):
        self._buf.clear()
        self._inside = False


def _looks_like_hex_text(sample: bytes) -> bool:
    if not sample:
        return False
    allowed = set(b"0123456789abcdefABCDEF \t\r\n")
    bad = sum(1 for ch in sample[:4096] if ch not in allowed)
    nul = sample[:4096].count(b"\x00")
    return bad == 0 and nul == 0


def _to_int_be(b: bytes) -> int:
    n = 0
    for x in b:
        n = (n << 8) | x
    return n


def _expand_bits(byte_val: int, bit_names: List[str], out: Dict[str, int], *, msb_first: bool = False):
    """
    Map a byte's bits to named fields. Default LSB-first (b0 = least-significant).
    Flip to msb_first=True if your naming expects MSB->LSB.
    """
    for i, name in enumerate(bit_names):
        bit = (byte_val >> (7 - i)) & 1 if msb_first else (byte_val >> i) & 1
        out[name] = bit


def _parse_payload(payload: bytes, schema: Dict) -> Dict:
    """
    Parse sequential fields according to schema["fields"].
    """
    out: Dict = {}
    idx = 0
    for fld in schema.get("fields", []):
        name = fld["name"]
        size = int(fld["size"])
        if idx + size > len(payload):
            raise ValueError(f"Payload too short for field {name}")
        chunk = payload[idx:idx + size]
        idx += size

        if size == 1:
            val = chunk[0]
            out[name] = val
            if "bits" in fld:
                _expand_bits(val, fld["bits"], out, msb_first=False)
        else:
            out[name] = _to_int_be(chunk)
    return out


def _parse_frame(frame: bytes, schema_map: Dict[int, Dict]) -> (bool, Dict):
    """
    Validate frame structure using schema_map; return (valid, data).
    Data contains parsed fields + ID.
    """
    if not frame or len(frame) < 6:
        return False, {}
    if frame[0] != START_FRAME or frame[-1] != END_FRAME:
        return False, {}

    pkt_id = int(frame[1])
    num_bytes = int(frame[2])

    schema = schema_map.get(pkt_id)
    if not schema:
        return False, {"ID": pkt_id}

    expected_len = int(schema.get("length", 0))
    if expected_len and expected_len != len(frame):
        return False, {"ID": pkt_id}

    if "num_bytes" in schema and num_bytes != int(schema["num_bytes"]):
        return False, {"ID": pkt_id}

    # payload = total - (START 1 + ID 1 + NUM 1 + CHK 2 + END 1)
    payload_len = len(frame) - 1 - 1 - 1 - 2 - 1
    if payload_len < 0:
        return False, {"ID": pkt_id}
    payload = frame[3:3 + payload_len]

    data = _parse_payload(payload, schema)
    data["ID"] = pkt_id
    return True, data


# ========== main parse ==========

def stream_to_parquet(job_id: str, dataset_id: str, raw_path: str, schema_file: str, batch_size: int = 5000):
    db = SessionLocal()
    parquet_path = None
    tmp_path = None
    writer = None
    try:
        set_status(job_id, JobStatus.running, progress=0.0, message="starting")
        append_log(job_id, "Parsing started", progress=0.0, message="starting")

        out_dir = os.path.dirname(raw_path)
        parquet_path = os.path.join(out_dir, "data.parquet")
        tmp_path = parquet_path + ".tmp"

        # Load schema map once
        with open(schema_file, "r") as f:
            schemas = json.load(f)
        schema_map: Dict[int, Dict] = {int(s["id"]): s for s in schemas}

        cols = compute_all_columns(schema_file)
        scanner = FrameScanner(START_FRAME, END_FRAME)

        packet_num = 0
        valid = 0
        batch: List[Dict] = []

        try:
            file_size = os.path.getsize(raw_path)
        except OSError:
            file_size = None
        bytes_read = 0

        def row_from_data(data: Dict) -> Dict:
            row = {c: None for c in cols}
            row["PacketNum"] = packet_num
            row["ID"] = data.get("ID")
            for k, v in data.items():
                if k in row:
                    row[k] = v
            return row

        with open(raw_path, "rb") as f:
            header = f.read(8192)
            bytes_read += len(header)
            hex_mode = _looks_like_hex_text(header)

            def feed_hex_bytes(chunk: bytes):
                for line in chunk.splitlines():
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        tokens = s.split()
                        data = bytes(int(t, 16) for t in tokens)
                    except Exception:
                        append_log(job_id, "Malformed hex line (ignored)")
                        continue
                    for frame in scanner.feed(data):
                        yield frame

            def flush_batch():
                nonlocal writer, batch
                if not batch:
                    return
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    # === tuned writer: smaller files & faster scans ===
                    writer = pq.ParquetWriter(
                        tmp_path,
                        table.schema,
                        compression="zstd",      # fall back to "snappy" if zstd not available
                        use_dictionary=True,
                        version="2.6",
                    )
                writer.write_table(table)
                batch.clear()

            if hex_mode:
                for frame in feed_hex_bytes(header):
                    ok, data = _parse_frame(frame, schema_map)
                    if not ok:
                        append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                        continue
                    packet_num += 1
                    batch.append(row_from_data(data))
                    valid += 1
                    if len(batch) >= batch_size:
                        flush_batch()
                        pct = None
                        if file_size:
                            pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                        append_log(job_id, f"Wrote {valid} packets", progress=pct, message="parsing")
                        set_status(job_id, JobStatus.running, progress=pct, message="parsing")

                for line in f:
                    bytes_read += len(line)
                    for frame in feed_hex_bytes(line):
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
                        valid += 1
                        if len(batch) >= batch_size:
                            flush_batch()
                            pct = None
                            if file_size:
                                pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                            append_log(job_id, f"Wrote {valid} packets", progress=pct, message="parsing")
                            set_status(job_id, JobStatus.running, progress=pct, message="parsing")

            else:
                if header:
                    for frame in scanner.feed(header):
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
                        valid += 1
                        if len(batch) >= batch_size:
                            flush_batch()

                while True:
                    chunk = f.read(4 * 1024 * 1024)  # 4MB
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    for frame in scanner.feed(chunk):
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
                        valid += 1
                        if len(batch) >= batch_size:
                            flush_batch()

                    pct = None
                    if file_size:
                        pct = min(90.0, round(bytes_read * 100.0 / file_size, 2))
                    set_status(job_id, JobStatus.running, progress=pct, message="parsing")

            scanner.flush()

        # final flush + atomic replace
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    tmp_path,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                    version="2.6",
                )
            writer.write_table(table)
            batch.clear()

        if writer:
            writer.close()
            os.replace(tmp_path, parquet_path)  # atomic on same filesystem

        # update dataset metadata
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
        # clean partial tmp on failure
        try:
            if writer:
                writer.close()
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
    finally:
        db.close()

    db = SessionLocal()
    try:
        set_status(job_id, JobStatus.running, progress=0.0, message="starting")
        append_log(job_id, "Parsing started", progress=0.0, message="starting")

        out_dir = os.path.dirname(raw_path)
        parquet_path = os.path.join(out_dir, "data.parquet")

        # Load schema map once
        with open(schema_file, "r") as f:
            schemas = json.load(f)
        schema_map: Dict[int, Dict] = {int(s["id"]): s for s in schemas}

        cols = compute_all_columns(schema_file)
        scanner = FrameScanner(START_FRAME, END_FRAME)

        packet_num = 0
        valid = 0
        batch: List[Dict] = []
        writer: Optional[pq.ParquetWriter] = None

        try:
            file_size = os.path.getsize(raw_path)
        except OSError:
            file_size = None
        bytes_read = 0

        def row_from_data(data: Dict) -> Dict:
            row = {c: None for c in cols}
            row["PacketNum"] = packet_num
            row["ID"] = data.get("ID")
            for k, v in data.items():
                if k in row:
                    row[k] = v
            return row

        with open(raw_path, "rb") as f:
            # Decide hex-dump vs binary
            header = f.read(8192)
            bytes_read += len(header)
            hex_mode = _looks_like_hex_text(header)

            def feed_hex_bytes(chunk: bytes):
                for line in chunk.splitlines():
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        tokens = s.split()
                        data = bytes(int(t, 16) for t in tokens)
                    except Exception:
                        append_log(job_id, "Malformed hex line (ignored)")
                        continue
                    for frame in scanner.feed(data):
                        yield frame

            if hex_mode:
                # consume header first
                for frame in feed_hex_bytes(header):
                    ok, data = _parse_frame(frame, schema_map)
                    if not ok:
                        append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                        continue
                    packet_num += 1
                    batch.append(row_from_data(data))
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

                # then the rest, line by line
                for line in f:
                    bytes_read += len(line)
                    for frame in feed_hex_bytes(line):
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
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
                # binary mode: feed raw bytes to scanner
                if header:
                    for frame in scanner.feed(header):
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
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
                        ok, data = _parse_frame(frame, schema_map)
                        if not ok:
                            append_log(job_id, f"Frame rejected (ID={data.get('ID')})")
                            continue
                        packet_num += 1
                        batch.append(row_from_data(data))
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
                    set_status(job_id, JobStatus.running, progress=pct, message="parsing")

            scanner.flush()

        # flush remaining
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
            batch.clear()
        if writer:
            writer.close()

        # update dataset metadata
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
