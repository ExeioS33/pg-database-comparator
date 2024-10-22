import os
from dotenv import load_dotenv

# Get the absolute path to the project root directory
project_root = os.path.dirname(os.path.abspath(__file__))

# Define the output directory path
output_dir = os.path.join(project_root, "data")

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# Charger les variables d'environnement depuis le fichier .env une seule fois
load_dotenv()

PG_BINARIES_PATH = os.environ.get("PG_DUMPS_PATH")
