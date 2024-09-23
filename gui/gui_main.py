# gui/gui_main.py

import tkinter as tk
from tkinter import messagebox
import os
import shutil
import sys

# Adjust the path to import from the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db.src.DBConnectionHandler import DbConnectionHandler
from db.src.DBComparator import DBObjectComparator


def clean_output_directory(directory):
    """Delete all files in the specified directory."""
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                # Delete files and directories
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        os.makedirs(directory)


class DatabaseComparatorGUI:
    def __init__(self, master):
        self.master = master
        master.title("Comparateur de base de données")

        # Labels and entries for database names
        self.label_db1 = tk.Label(master, text="Nom du serveur de la première DB (ex: PG-TEST):")
        self.label_db1.grid(row=0, column=0, padx=5, pady=5)
        self.entry_db1 = tk.Entry(master)
        self.entry_db1.grid(row=0, column=1, padx=5, pady=5)

        self.label_db2 = tk.Label(master, text="Nom du serveur de la seconde DB (ex: PG-DWH):")
        self.label_db2.grid(row=1, column=0, padx=5, pady=5)
        self.entry_db2 = tk.Entry(master)
        self.entry_db2.grid(row=1, column=1, padx=5, pady=5)

        # Label and entry for schema
        self.label_schema = tk.Label(master, text="Nom du schema (optionnel):")
        self.label_schema.grid(row=2, column=0, padx=5, pady=5)
        self.entry_schema = tk.Entry(master)
        self.entry_schema.grid(row=2, column=1, padx=5, pady=5)

        # Compare button
        self.compare_button = tk.Button(master, text="Comparer", command=self.compare_databases)
        self.compare_button.grid(row=3, column=0, columnspan=2, pady=10)

        # Text area for output messages
        self.text_output = tk.Text(master, height=10, width=60)
        self.text_output.grid(row=4, column=0, columnspan=2, padx=5, pady=5)

    def compare_databases(self):
        db1_name = self.entry_db1.get()
        db2_name = self.entry_db2.get()
        schema_name = self.entry_schema.get() or None

        # Clear the output text
        self.text_output.delete(1.0, tk.END)

        try:
            # Clean the data directory before comparison
            output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
            clean_output_directory(output_dir)
            self.text_output.insert(tk.END, "Dossier data nettoyé.\n")

            # Initialize the database connection handler
            db_handler = DbConnectionHandler(db1_name, db2_name)
            connections = db_handler.get_connections()

            conn1 = connections.get(db1_name)
            conn2 = connections.get(db2_name)

            # Initialize the comparator
            comparator = DBObjectComparator(conn1, conn2, schema=schema_name)

            # Perform the comparison
            comparator.compare_objects()

            # Close the connections
            db_handler.close_connections()

            self.text_output.insert(tk.END, "Comparaison effectue avec succes.\n")
            self.text_output.insert(tk.END, "Regarder le dossier 'data' pour voir les CSV de comparaisons.")

        except Exception as e:
            messagebox.showerror("Erreur", str(e))
            self.text_output.insert(tk.END, f"Une erreur est survenue: {e}")


if __name__ == '__main__':
    root = tk.Tk()
    gui = DatabaseComparatorGUI(root)
    root.mainloop()
