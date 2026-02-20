import json
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from app.zim_xfa import analyze_pdf, fill_pdf

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/zim/analyze")
async def analyze_zim_pdf(file: UploadFile = File(...), payload: str = Form("{}")):
    try:
        payload_data = json.loads(payload or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Ungültiges payload JSON: {exc}") from exc

    pdf_bytes = await file.read()
    try:
        return analyze_pdf(pdf_bytes, payload_data)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/zim/fill")
async def fill_zim_pdf(
    file: UploadFile = File(...),
    payload: str = Form("{}"),
    confirmMismatch: bool = Form(False),
):
    try:
        payload_data = json.loads(payload or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Ungültiges payload JSON: {exc}") from exc

    pdf_bytes = await file.read()
    try:
        output_pdf, meta = fill_pdf(pdf_bytes, payload_data, confirm_mismatch=confirmMismatch)
    except PermissionError:
        preview = analyze_pdf(pdf_bytes, payload_data)
        return JSONResponse(status_code=409, content={
            "detail": "Akronym in PDF weicht vom Projektnamen ab.",
            "pdfAcronym": preview.get("pdfAcronym"),
            "projectName": preview.get("projectName"),
            "acronymMismatch": True,
        })
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    filename = meta.get("downloadName") or "Mantelbogen.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=output_pdf, media_type="application/pdf", headers=headers)


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def root():
    if INDEX_FILE.exists():
        return FileResponse(str(INDEX_FILE))
    return {"detail": "Frontend not built"}


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")
    target = STATIC_DIR / full_path
    if target.exists() and target.is_file():
        return FileResponse(str(target))
    if INDEX_FILE.exists():
        return FileResponse(str(INDEX_FILE))
    return {"detail": "Frontend not built"}
