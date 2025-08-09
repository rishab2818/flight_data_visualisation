import os, json
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Body
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from db.session import SessionLocal
from models import Dataset, User, PlotPreset
from security import decode_token
from services.plot_service import build_plot_html, build_overlay_plot_html

router = APIRouter(prefix="/plots", tags=["plots"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def auth_or_token(authorization: str = Header(None), token: str | None = Query(None), db: Session = Depends(get_db)) -> User | None:
    raw = None
    if authorization and authorization.lower().startswith("bearer "):
        raw = authorization.split(" ",1)[1].strip()
    elif token:
        raw = token
    if not raw:
        return None
    payload = decode_token(raw)
    if not payload:
        raise HTTPException(status_code=401, detail="invalid token")
    uid = int(payload.get("sub"))
    u = db.query(User).get(uid)
    if not u or not u.active:
        raise HTTPException(status_code=401, detail="inactive user")
    return u

@router.get("/plot")
def plot(
    dataset_id: str, x_col: str, y_col: str,
    method: str = Query("stride", regex="^(stride|lttb)$"),
    max_points: int = Query(100_000, ge=1000, le=2_000_000),
    computes: str | None = None, filters: str | None = None,
    user: User | None = Depends(auth_or_token), db: Session = Depends(get_db)
):
    ds = db.query(Dataset).filter(Dataset.id==dataset_id).first()
    if not ds: raise HTTPException(status_code=404, detail="not found")
    if not ds.parquet_path or not os.path.exists(ds.parquet_path): raise HTTPException(status_code=400, detail="parquet not ready")
    comp_list = json.loads(computes) if computes else None
    filt_list = json.loads(filters) if filters else None
    out = build_plot_html(ds.parquet_path, x_col, y_col, f"{x_col} vs {y_col}", os.path.join(os.path.dirname(ds.parquet_path), "plots"), method=method, max_points=max_points, computes=comp_list, filters=filt_list)
    return FileResponse(out, filename=os.path.basename(out), media_type='text/html', headers={'Content-Disposition': f"inline; filename={os.path.basename(out)}"})

@router.post("/overlay")
def overlay(
    payload: dict = Body(...),
    user: User | None = Depends(auth_or_token),
    db: Session = Depends(get_db),
):
    method = payload.get("method","stride")
    max_points = int(payload.get("max_points", 100_000))
    x_col = payload.get("x_col")
    series_in = payload.get("series") or []
    if not x_col or not series_in:
        raise HTTPException(status_code=400, detail="x_col and series are required")
    concrete = []
    first_dir = None
    for s in series_in:
        ds = db.query(Dataset).filter(Dataset.id==s.get("dataset_id")).first()
        if not ds or not ds.parquet_path or not os.path.exists(ds.parquet_path):
            raise HTTPException(status_code=404, detail=f"dataset not ready: {s.get('dataset_id')}")
        if first_dir is None: first_dir = os.path.dirname(ds.parquet_path)
        concrete.append({
            "parquet_path": ds.parquet_path,
            "x_col": x_col,
            "y_col": s.get("y_col"),
            "label": s.get("label"),
            "filters": s.get("filters"),
            "computes": s.get("computes")
        })
    title = payload.get("title") or f"{x_col} overlay"
    out = build_overlay_plot_html(concrete, title, os.path.join(first_dir, "plots"), method=method, max_points=max_points)
    return FileResponse(out, filename=os.path.basename(out), media_type='text/html', headers={'Content-Disposition': f"inline; filename={os.path.basename(out)}"})

@router.post("/presets")
def save_preset(payload: dict = Body(...), user: User | None = Depends(auth_or_token), db: Session = Depends(get_db)):
    if not user: raise HTTPException(status_code=401, detail="auth required")
    name = payload.get("name"); config = payload.get("config")
    if not name or not config: raise HTTPException(status_code=400, detail="name and config required")
    p = PlotPreset(owner_id=user.id, name=name, description=payload.get("description"), config_json=json.dumps(config))
    db.add(p); db.commit(); db.refresh(p)
    return {"id": p.id, "name": p.name, "description": p.description, "created_at": p.created_at.isoformat()}

@router.get("/presets")
def list_presets(user: User | None = Depends(auth_or_token), db: Session = Depends(get_db)):
    if not user: raise HTTPException(status_code=401, detail="auth required")
    rows = db.query(PlotPreset).filter(PlotPreset.owner_id==user.id).order_by(PlotPreset.created_at.desc()).all()
    return [{"id": r.id, "name": r.name, "description": r.description, "config": json.loads(r.config_json), "created_at": r.created_at.isoformat()} for r in rows]

@router.delete("/presets/{preset_id}")
def delete_preset(preset_id: int, user: User | None = Depends(auth_or_token), db: Session = Depends(get_db)):
    if not user: raise HTTPException(status_code=401, detail="auth required")
    p = db.query(PlotPreset).filter(PlotPreset.id==preset_id, PlotPreset.owner_id==user.id).first()
    if not p: raise HTTPException(status_code=404, detail="not found")
    db.delete(p); db.commit()
    return {"ok": True}
