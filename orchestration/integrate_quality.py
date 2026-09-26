
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

pipeline_path = BASE_DIR / "state_machine.asl.json"
quality_path = BASE_DIR / "quality_check.asl.json"
output_path = BASE_DIR / "state_machine_v2.asl.json"

# 1. Leer las dos máquinas existentes
with pipeline_path.open(encoding="utf-8-sig") as f:
    pipeline = json.load(f)

with quality_path.open(encoding="utf-8-sig") as f:
    quality = json.load(f)

# 2. Comprobar que las conexiones esperadas existen
crawler_choices = pipeline["States"]["CrawlerStatus"]["Choices"]

success_rules = [
    rule for rule in crawler_choices
    if rule.get("Next") == "PipelineSucceeded"
]

assert len(success_rules) == 1, (
    "No se encontró una única ruta de éxito del crawler"
)

new_states = quality["States"]

assert all(
    name not in pipeline["States"]
    for name in new_states
), "Algunos estados de calidad ya existen en el pipeline"

# 3. Conectar el crawler con Athena
success_rules[0]["Next"] = "RunQualityQuery"

# 4. Incorporar los estados de calidad
pipeline["States"].update(new_states)

# 5. Después de PASS, finalizar el pipeline principal
pipeline["States"]["QualityPassed"] = {
    "Type": "Pass",
    "Next": "PipelineSucceeded"
}

# 6. Guardar una versión nueva, sin sobrescribir la anterior
with output_path.open("w", encoding="utf-8") as f:
    json.dump(pipeline, f, indent=2, ensure_ascii=False)

print(f"Created: {output_path}")
print(f"Total states: {len(pipeline['States'])}")