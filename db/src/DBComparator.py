import csv
import os
from abc import ABC, abstractmethod
from typing import Any
try:
    from config import output_dir
except ImportError:
    # Fallback if config.py is not found
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

from db.schemas.db_objects import DBObjects


class Comparator(ABC):
    @abstractmethod
    def compare_objects(self):
        """Abstract method to compare objects between two databases."""
        pass


def _write_differences_to_csv(differences, object_type, identifier_keys, output_dir):
    """Write differences to a CSV file with specified columns in French."""
    # Define the CSV file name
    csv_file = os.path.join(output_dir, f"{object_type}_differences.csv")

    # Define the header columns
    # Base columns
    base_columns = ['Type', 'Etat', 'source', 'schema', 'nom']

    # Additional columns based on identifier_keys, excluding 'schema', 'nom', 'name'
    additional_columns = [key for key in identifier_keys if key not in ['schema', 'nom', 'name']]

    # For certain object types, include additional fields
    if object_type in ['function', 'procedure']:
        # Avoid adding duplicates
        for field in ['arguments', 'definition']:
            if field not in additional_columns:
                additional_columns.append(field)
    elif object_type in ['trigger', 'index']:
        if 'definition' not in additional_columns:
            additional_columns.append('definition')
    elif object_type == 'column':
        for field in ['data_type', 'is_nullable', 'column_default']:
            if field not in additional_columns:
                additional_columns.append(field)

    # Remove duplicates from additional_columns
    additional_columns = list(dict.fromkeys(additional_columns))

    # Translate columns to French
    column_translations = {
        'Type': 'type',
        'Etat': 'etat',
        'source': 'source',
        'schema': 'schema',
        'nom': 'nom',
        'arguments': 'arguments',
        'definition': 'definition',
        'table_name': 'nom_table',
        'column_name': 'nom_colonne',
        'data_type': 'type_donnees',
        'is_nullable': 'est_nullable',
        'column_default': 'valeur_par_defaut',
        'name': 'nom',
        'table': 'table',
        'indexname': 'nom_index',
        'indexdef': 'definition_index',
    }

    header_columns = [column_translations.get(col, col) for col in base_columns + additional_columns]

    # Write to CSV
    try:
        with open(csv_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(header_columns)
            # Write data
            for diff in differences:
                row = []
                for col in base_columns + additional_columns:
                    row.append(diff.get(col, ''))
                writer.writerow(row)
        print(f"Successfully wrote differences to {csv_file}")
    except Exception as e:
        print(f"Failed to write CSV file {csv_file}. Reason: {e}")


class DBObjectComparator(Comparator):
    def __init__(self, db1_conn, db2_conn, schema: Any | str = None):
        self.db1_conn = db1_conn
        self.db2_conn = db2_conn
        self.schema = schema
        self.output_dir = output_dir

    def compare_schema_objects(self):
        """Compare objects within the specified schema in both databases."""
        # Compare tables
        tables1 = DBObjects.get_tables(self.db1_conn, self.schema)
        tables2 = DBObjects.get_tables(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=tables1,
            objects2=tables2,
            identifier_keys=['schema', 'name'],
            object_type='table'
        )

        # Compare columns
        columns1 = DBObjects.get_columns(self.db1_conn, self.schema)
        columns2 = DBObjects.get_columns(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=columns1,
            objects2=columns2,
            identifier_keys=['schema', 'table_name', 'column_name'],
            object_type='column'
        )

        # Compare indexes
        indexes1 = DBObjects.get_indexes(self.db1_conn, self.schema)
        indexes2 = DBObjects.get_indexes(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=indexes1,
            objects2=indexes2,
            identifier_keys=['schema', 'table_name', 'name'],
            object_type='index'
        )

        # Compare functions
        functions1 = DBObjects.get_functions(self.db1_conn, self.schema)
        functions2 = DBObjects.get_functions(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=functions1,
            objects2=functions2,
            identifier_keys=['schema', 'name', 'arguments'],
            object_type='function'
        )

        # Compare procedures
        procedures1 = DBObjects.get_procedures(self.db1_conn, self.schema)
        procedures2 = DBObjects.get_procedures(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=procedures1,
            objects2=procedures2,
            identifier_keys=['schema', 'name', 'arguments'],
            object_type='procedure'
        )

        # Compare triggers
        triggers1 = DBObjects.get_triggers(self.db1_conn, self.schema)
        triggers2 = DBObjects.get_triggers(self.db2_conn, self.schema)
        self._compare_objects_generic(
            objects1=triggers1,
            objects2=triggers2,
            identifier_keys=['schema', 'table_name', 'name'],
            object_type='trigger'
        )

    def compare_objects(self):
        """Implement the required method from the Comparator interface."""
        self.compare_schema_objects()

    def _compare_objects_generic(self, objects1, objects2, identifier_keys, object_type):
        """Generic method to compare objects and collect differences."""
        dict1 = {tuple(obj[k] for k in identifier_keys): obj for obj in objects1}
        dict2 = {tuple(obj[k] for k in identifier_keys): obj for obj in objects2}

        only_in_db1 = dict1.keys() - dict2.keys()
        only_in_db2 = dict2.keys() - dict1.keys()
        in_both = dict1.keys() & dict2.keys()

        differences = []

        # Objects only in db1
        for obj_id in only_in_db1:
            obj = dict1[obj_id]
            difference = {
                'Type': object_type,
                'Etat': 'unique',
                'source': 'preprod',
                'schema': obj.get('schema', ''),
                'nom': obj.get('name', obj.get('nom', ''))
            }
            # Add additional identifier_keys to difference
            for key in identifier_keys:
                if key not in ['schema', 'name', 'nom']:
                    difference[key] = obj.get(key, '')
            # Add 'definition' if applicable
            if 'definition' in obj:
                difference['definition'] = obj.get('definition', '')
            differences.append(difference)
            if object_type == 'column':
                difference['data_type'] = obj.get('data_type', '')
                difference['is_nullable'] = obj.get('is_nullable', '')
                difference['column_default'] = obj.get('column_default', '')
            differences.append(difference)

        # Objects only in db2
        for obj_id in only_in_db2:
            obj = dict2[obj_id]
            difference = {
                'Type': object_type,
                'Etat': 'unique',
                'source': 'prod',
                'schema': obj.get('schema', ''),
                'nom': obj.get('name', obj.get('nom', ''))
            }
            # Add additional identifier_keys to difference
            for key in identifier_keys:
                if key not in ['schema', 'name', 'nom']:
                    difference[key] = obj.get(key, '')
            # Add 'definition' if applicable
            if 'definition' in obj:
                difference['definition'] = obj.get('definition', '')
            differences.append(difference)
            if object_type == 'column':
                difference['data_type'] = obj.get('data_type', '')
                difference['is_nullable'] = obj.get('is_nullable', '')
                difference['column_default'] = obj.get('column_default', '')
            differences.append(difference)

        # Objects in both but different
        for obj_id in in_both:
            obj1 = dict1[obj_id]
            obj2 = dict2[obj_id]
            if obj1 != obj2:
                # Entry for db1
                difference1 = {
                    'Type': object_type,
                    'Etat': 'difference',
                    'source': 'preprod',
                    'schema': obj1.get('schema', ''),
                    'nom': obj1.get('name', obj1.get('nom', ''))
                }
                # Add additional identifier_keys to difference
                for key in identifier_keys:
                    if key not in ['schema', 'name', 'nom']:
                        difference1[key] = obj1.get(key, '')
                # Add 'definition' if applicable
                if 'definition' in obj1:
                    difference1['definition'] = obj1.get('definition', '')
                differences.append(difference1)
                if object_type == 'column':
                    difference1['data_type'] = obj1.get('data_type', '')
                    difference1['is_nullable'] = obj1.get('is_nullable', '')
                    difference1['column_default'] = obj1.get('column_default', '')
                differences.append(difference1)
                difference2 = {
                    'Type': object_type,
                    'Etat': 'difference',
                    'source': 'prod',
                    'schema': obj2.get('schema', ''),
                    'nom': obj2.get('name', obj2.get('nom', ''))
                }
                # Add additional identifier_keys to difference
                for key in identifier_keys:
                    if key not in ['schema', 'name', 'nom']:
                        difference2[key] = obj2.get(key, '')
                # Add 'definition' if applicable
                if 'definition' in obj2:
                    difference2['definition'] = obj2.get('definition', '')
                differences.append(difference2)
                if object_type == 'column':
                    difference2['data_type'] = obj2.get('data_type', '')
                    difference2['is_nullable'] = obj2.get('is_nullable', '')
                    difference2['column_default'] = obj2.get('column_default', '')
                differences.append(difference2)

        # Remove duplicates from differences
        unique_differences = []
        seen = set()
        for diff in differences:
            diff_tuple = tuple(sorted(diff.items()))
            if diff_tuple not in seen:
                seen.add(diff_tuple)
                unique_differences.append(diff)
        differences = unique_differences

        # Write differences to CSV
        if differences:
            _write_differences_to_csv(differences, object_type, identifier_keys, self.output_dir)
