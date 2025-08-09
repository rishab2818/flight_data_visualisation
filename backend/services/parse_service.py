import os, json, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from datetime import datetime
from db.session import SessionLocal
from models import Dataset, Job, JobStatus
import packet_parser

def compute_all_columns(schema_path: str):
    with open(schema_path, 'r') as f:
        schemas = json.load(f)
    all_fields = set(['PacketNum', 'ID'])
    for schema in schemas:
        all_fields.update(schema.get('all_fields', []))
        for field in schema['fields']:
            all_fields.add(field['name'])
            if 'bits' in field:
                all_fields.update(field['bits'])
    return sorted(list(all_fields))

def append_log(job_id: str, text: str, progress: float|None=None, message: str|None=None):
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id==job_id).first()
        if not j: return
        ts = datetime.utcnow().isoformat()
        j.logs = (j.logs or "") + f"[{ts}] {text}\n"
        if progress is not None: j.progress = progress
        if message is not None: j.message = message
        if j.status == JobStatus.pending: j.status = JobStatus.running
        db.add(j); db.commit()
    finally: db.close()

def set_status(job_id: str, status: JobStatus, progress: float|None=None, message: str|None=None):
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id==job_id).first()
        if not j: return
        j.status = status
        if progress is not None: j.progress = progress
        if message is not None: j.message = message
        if status in (JobStatus.success, JobStatus.failed):
            j.finished_at = datetime.utcnow()
        db.add(j); db.commit()
    finally: db.close()

def stream_to_parquet(job_id: str, dataset_id: str, raw_path: str, schema_file: str):
    db = SessionLocal()
    try:
        append_log(job_id, "Starting parse", progress=1.0, message="started")
        cols = compute_all_columns(schema_file)
        parquet_path = os.path.join(os.path.dirname(raw_path), "parsed.parquet")
        writer = None
        batch=[]; batch_size=1000; valid=0
        factory = packet_parser.PacketFactory(schema_file)
        packet_num = 0
        with open(raw_path, "r", encoding="utf-8", errors="ignore") as rf:
            for line in rf:
                packet_num += 1
                s = line.strip()
                if not s: continue
                try:
                    packet_bytes = [int(b,16) for b in s.split()]
                except Exception:
                    append_log(job_id, f"Malformed line {packet_num}")
                    continue
                pkt = factory.create_packet(packet_bytes)
                if not pkt:
                    append_log(job_id, f"Unknown ID line {packet_num}")
                    continue
                pkt.parse()
                if not getattr(pkt, "valid", False):
                    append_log(job_id, f"Invalid packet line {packet_num}")
                    continue
                row = pkt.data.copy(); row["PacketNum"] = packet_num
                for c in cols:
                    if c not in row: row[c]=None
                batch.append(row); valid += 1
                if len(batch) >= batch_size:
                    df = pd.DataFrame(batch, columns=cols)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_path, table.schema)
                    writer.write_table(table); batch.clear()
                if packet_num % 5000 == 0:
                    append_log(job_id, f"Processed {packet_num}, valid {valid}", progress=min(90.0, 5 + packet_num/10000*10))
        if batch:
            df = pd.DataFrame(batch, columns=cols)
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table); batch.clear()
        if writer: writer.close()
        d = db.query(Dataset).filter(Dataset.id==dataset_id).first()
        if d:
            d.parquet_path = parquet_path
            d.columns_json = json.dumps(cols)
            d.packet_count = valid
            db.add(d); db.commit()
        append_log(job_id, f"Completed rows={valid}", progress=95.0, message="parsed")
        set_status(job_id, JobStatus.success, progress=100.0, message="completed")
    except Exception as e:
        append_log(job_id, f"Error: {e}", progress=100.0, message="failed")
        set_status(job_id, JobStatus.failed, progress=100.0, message=str(e))
    finally: db.close()
