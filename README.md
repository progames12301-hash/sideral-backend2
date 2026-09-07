# Sideral Satellite Backend

Serviço independente para dados brutos NOAA GOES-19 ABI. Não depende do
`server.py` do backend principal.

## Executar localmente

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Endpoints principais:

- `GET /api/health`
- `GET /api/satellite/status`
- `GET /api/satellite/products`
- `GET /api/satellite/latest?product=C13`
- `GET /api/satellite/frames?product=C13&limit=36`
- `GET /api/satellite/image?product=ir&key=...&bbox=-90,-60,-30,15&width=1024`
- `GET /api/satellite/original?key=...`

O serviço consulta o catálogo público NOAA/NODD, baixa somente o NetCDF
necessário, mantém cache local e reprojeta C13/C02 para uma grade Web Mercator
com amostragem nearest-neighbor.
