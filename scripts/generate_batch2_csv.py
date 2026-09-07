"""Generate a second deliveries CSV for alternate-file pipeline tests."""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
random.seed(42)


def main() -> None:
    mats = pd.read_csv(ROOT / "raw" / "materials_catalog.csv")
    valid_mats = mats["material"].unique().tolist()
    orphan_mats = [f"XX{n}" for n in range(880001, 880021)]

    paises = ["SV", "HN", "EC", "GT", "JM", "PE"]
    tipos_ok = ["ZPRE", "ZVE1", "Z04", "Z05"]
    tipos_bad = ["COBR", "Z99"]
    unidades = ["CS", "ST"]

    n = 350
    rows: list[dict] = []
    for _ in range(n):
        kind = random.random()
        pais = random.choice(paises)
        day = random.randint(1, 31)
        fecha: str | None = f"202507{day:02d}"
        transporte = random.randint(60_000_000, 160_000_000)
        ruta = random.randint(900_000, 999_999)
        unidad = random.choice(unidades)
        precio: float | None = round(random.uniform(15.0, 65.0), 12)
        cantidad = float(random.randint(1, 150))
        tipo = random.choice(tipos_ok)
        material = random.choice(valid_mats)

        if kind < 0.03:
            fecha = None
        elif kind < 0.04:
            fecha = "2025-07-01"
        elif kind < 0.07:
            cantidad = 0.0 if random.random() < 0.5 else -float(random.randint(1, 20))
        elif kind < 0.10:
            material = random.choice(orphan_mats)
        elif kind < 0.14:
            tipo = random.choice(tipos_bad)
        elif kind < 0.16:
            precio = None

        rows.append(
            {
                "pais": pais,
                "fecha_proceso": fecha,
                "transporte": transporte,
                "ruta": ruta,
                "tipo_entrega": tipo,
                "material": material,
                "precio": precio,
                "cantidad": cantidad,
                "unidad": unidad,
            }
        )

    for r in rows[:5]:
        rows.append(dict(r))

    out = pd.DataFrame(rows)
    path = ROOT / "raw" / "global_mobility_data_entrega_productos_batch2.csv"
    out.to_csv(path, index=False)
    print(f"wrote {path} rows={len(out)}")
    print(out["pais"].value_counts().to_string())
    print("null fecha", out["fecha_proceso"].isna().sum())
    print("qty<=0", (out["cantidad"].fillna(0) <= 0).sum())
    print("orphan", out["material"].astype(str).str.startswith("XX").sum())
    print("bad tipo", out["tipo_entrega"].isin(["COBR", "Z99"]).sum())
    print("null precio", out["precio"].isna().sum())


if __name__ == "__main__":
    main()
