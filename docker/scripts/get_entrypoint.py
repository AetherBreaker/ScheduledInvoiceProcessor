# Standard library imports
import tomllib

with open("/app/pyproject.toml", "rb") as f:
  data = tomllib.load(f)

project_name = data["project"]["name"].replace("_", "-")
print(project_name, end="")
