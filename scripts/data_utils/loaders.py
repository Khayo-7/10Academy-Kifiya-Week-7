import os
import csv
import yaml
import json
import joblib
import pandas as pd
from bson import ObjectId
from typing import List, Dict, Union, Any
from scripts.utils.logger import setup_logger

# Setup logger for data_loader
logger = setup_logger("data_loader")

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

import os
import csv
import yaml
import json
import joblib
import pandas as pd
from typing import List, Dict, Union, Optional, Any
from functools import wraps
import sys
sys.path.append("..")
sys.path.append("../..")
from scripts.utils.logger import setup_logger

# Setup logger for data_loader
logger = setup_logger("data_loader")

# Configuration (can be loaded from a YAML file)
CONFIG = {
    "conll": {
        "columns": ["tokens", "labels"]
    }
}

# Decorator for handling common file operations
def handle_file_operations(func):
    @wraps(func)
    def wrapper(file_path: str, *args, **kwargs):
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        logger.info(f"Loading data from {file_path}")
        try:
            result = func(file_path, *args, **kwargs)
            logger.info(f"Successfully loaded data from {file_path}")
            return result
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            raise
    return wrapper

@handle_file_operations
def load_yml(file_path: str) -> Any:
    """
    Load a YAML file into a Python object.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        Any: Loaded data.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        return yaml.safe_load(file)

@handle_file_operations
def load_csv(file_path: str, delimiter: str = ',', use_pandas: bool = True, chunksize: Optional[int] = None) -> Union[List[Dict[str, str]], pd.DataFrame]:
    """
    Load a CSV file into a list of dictionaries or a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.
        delimiter (str, optional): Delimiter to use. Defaults to ','.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.
        chunksize (Optional[int], optional): Number of rows per chunk. Defaults to None.

    Returns:
        Union[List[Dict[str, str]], pd.DataFrame]: Loaded data.
    """
    if use_pandas:
        if chunksize:
            return pd.read_csv(file_path, delimiter=delimiter, chunksize=chunksize)
        return pd.read_csv(file_path, delimiter=delimiter)
    else:
        with open(file_path, mode='r', encoding='utf-8') as file:
            return list(csv.DictReader(file, delimiter=delimiter))

@handle_file_operations
def load_json(file_path: str, use_pandas: bool = False) -> Union[List[Dict], Dict, pd.DataFrame]:
    """
    Load a JSON file into a Python object or a pandas DataFrame.

    Args:
        file_path (str): Path to the JSON file.
        use_pandas (bool, optional): Whether to use pandas. Defaults to False.

    Returns:
        Union[List[Dict], Dict, pd.DataFrame]: Loaded data.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        data = json.load(file)
    if use_pandas:
        return pd.DataFrame(data)
    return data

@handle_file_operations
def load_pickle(file_path: str) -> Any:
    """
    Load a pickle file into a Python object.

    Args:
        file_path (str): Path to the pickle file.

    Returns:
        Any: Loaded data.
    """
    return joblib.load(file_path)

@handle_file_operations
def load_excel(file_path: str, sheet_name: Optional[str] = None, use_pandas: bool = True) -> Union[List[Dict], pd.DataFrame]:
    """
    Load data from an Excel file into a list of dictionaries or a pandas DataFrame.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (Optional[str], optional): Name of the sheet to load. Defaults to None.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.

    Returns:
        Union[List[Dict], pd.DataFrame]: Loaded data.
    """

    data = pd.read_excel(file_path, sheet_name=sheet_name)
    if use_pandas:
        return data
    return data.to_dict("records")

@handle_file_operations
def load_conll(file_path: str, columns: Optional[List[str]] = None, use_pandas: bool = True) -> Union[List[Dict[str, List[str]]], pd.DataFrame]:
    """
    Load data from a CoNLL file into a list of dictionaries or a pandas DataFrame.

    Args:
        file_path (str): Path to the CoNLL file.
        columns (Optional[List[str]], optional): Column names for the data. Defaults to ["tokens", "labels"].
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.

    Returns:
        Union[List[Dict[str, List[str]]], pd.DataFrame]: Loaded data.
    """
    if columns is None:
        columns = CONFIG["conll"]["columns"]
    data, tokens, labels = [], [], []
    tokens_column, labels_column = columns

    with open(file_path, mode='r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:
                token, label = line.split("\t")
                tokens.append(token)
                labels.append(label)
            else:
                if tokens and labels:
                    data.append({tokens_column: tokens, labels_column: labels})
                    tokens, labels = [], []
        if tokens and labels:
            data.append({tokens_column: tokens, labels_column: labels})

    if use_pandas:
        return pd.DataFrame(data)
    return data

def save_csv(data: Union[List[Dict[str, str]], pd.DataFrame], output_path: str, delimiter: str = ',', use_pandas: bool = True) -> None:
    """
    Save data to a CSV file.

    Args:
        data (Union[List[Dict[str, str]], pd.DataFrame]): Data to save.
        output_path (str): Path to save the CSV file.
        delimiter (str, optional): Delimiter to use. Defaults to ','.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if use_pandas:
        if isinstance(data, pd.DataFrame):
            data.to_csv(output_path, index=False, sep=delimiter)
        else:
            pd.DataFrame(data).to_csv(output_path, index=False, sep=delimiter)
    else:
        with open(output_path, mode='w', encoding='utf-8', newline='') as file:
            if data:
                writer = csv.DictWriter(file, fieldnames=data[0].keys(), delimiter=delimiter)
                writer.writeheader()
                writer.writerows(data)
    logger.info(f"Data saved to {output_path}")

def save_excel(data: Union[List[Dict[str, str]], pd.DataFrame], output_path: str, sheet_name: str = 'Sheet1', use_pandas: bool = True) -> None:
    """
    Save data to an Excel file.

    Args:
        data (Union[List[Dict[str, str]], pd.DataFrame]): Data to save.
        output_path (str): Path to save the Excel file.
        sheet_name (str, optional): Name of the sheet to save. Defaults to 'Sheet1'.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Ensure data is a DataFrame or a list of dictionaries
    if isinstance(data, pd.DataFrame):
        data.to_excel(output_path, index=False, sheet_name=sheet_name)
    elif isinstance(data, list):
        # Convert list of dicts to DataFrame
        if all(isinstance(item, dict) for item in data):
            pd.DataFrame(data).to_excel(output_path, index=False, sheet_name=sheet_name)
        else:
            # Handle case where data is scalar or incorrectly structured
            raise ValueError("List elements must be dictionaries.")
    elif isinstance(data, dict):
        # Convert a scalar dictionary to a DataFrame
        pd.DataFrame([{k: [v]} for k, v in data.items()]).to_excel(output_path, index=False, sheet_name=sheet_name)
    else:
        logger.warning("Data format not supported. Must be a DataFrame, list of dictionaries, or a dictionary.")
        raise ValueError("Unsupported data format. Please provide a DataFrame, a list of dictionaries, or a dictionary.")

    logger.info(f"Data saved to {output_path}")

def save_json(data: Union[List[Dict], Dict, pd.DataFrame], output_path: str, use_pandas: bool = True) -> None:
    """
    Save data to a JSON file.

    Args:
        data (Union[List[Dict], Dict, pd.DataFrame]): Data to save.
        output_path (str): Path to save the JSON file.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if use_pandas:
        if isinstance(data, pd.DataFrame):
            data.to_json(output_path, orient="records", lines=True, force_ascii=False)
        else:
            pd.DataFrame(data).to_json(output_path, orient="records", lines=True, force_ascii=False)
    else:
        with open(output_path, mode='w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4, cls=CustomJSONEncoder)
    logger.info(f"Data saved to {output_path}")

def save_pickle(data: Any, output_path: str) -> None:
    """
    Save data to a pickle file.

    Args:
        data (Any): Data to save.
        output_path (str): Path to save the pickle file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(data, output_path)
    logger.info(f"Data saved to {output_path}")

def save_conll(data: Union[List[Dict[str, List[str]]], pd.DataFrame], output_path: str, columns: Optional[List[str]] = None) -> None:
    """
    Save data to a CoNLL file.

    Args:
        data (Union[List[Dict[str, List[str]]], pd.DataFrame]): Data to save.
        output_path (str): Path to save the CoNLL file.
        columns (Optional[List[str]], optional): Column names for the data. Defaults to ["tokens", "labels"].
    """
    if columns is None:
        columns = CONFIG["conll"]["columns"]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data = data.to_dict("records")

    with open(output_path, mode='w', encoding='utf-8') as file:
        for row in data:
            tokens = row[columns[0]]
            labels = row[columns[1]] if len(columns) > 1 else [""] * len(tokens)
            for token, label in zip(tokens, labels):
                file.write(f"{token}\t{label}\n")
            file.write("\n")
    logger.info(f"Data saved to {output_path}")

def load_data(file_path: str, use_pandas: bool = True, **kwargs) -> Union[List[Dict], Dict, pd.DataFrame]:
    """
    Load data from a JSON, CSV, Excel, CoNLL, or pickle file.

    Args:
        file_path (str): Path to the file.
        use_pandas (bool, optional): Whether to use pandas. Defaults to True.

    Returns:
        Union[List[Dict], Dict, pd.DataFrame]: Loaded data.

    Raises:
        ValueError: If the file format is unsupported.
    """
    ext = os.path.splitext(file_path)[1].lower()    
    LOADERS = {
        ".json": load_json,
        ".csv": load_csv,
        ".xlsx": load_excel,
        ".pkl": load_pickle,
        ".conll": load_conll,
    }
    if ext not in LOADERS:
        logger.error(f"Unsupported file format: {ext}")
        raise ValueError(f"Unsupported file format: {ext}")
    return LOADERS[ext](file_path, use_pandas=use_pandas, **kwargs)

def save_data(data: Any, output_path: str, **kwargs) -> None:
    """
    Save data to a JSON, CSV, Excel, CoNLL, or pickle file based on the file extension.

    Args:
        data (Any): Data to save.
        output_path (str): Path to save the file.
        **kwargs: Additional arguments to pass to the specific save function.

    Raises:
        ValueError: If the file format is unsupported.
    """
    ext = os.path.splitext(output_path)[1].lower()
    SAVERS = {
        ".json": save_json,
        ".csv": save_csv,
        ".xlsx": save_excel,
        ".pkl": save_pickle,
        ".conll": save_conll,
    }
    if ext not in SAVERS:
        logger.error(f"Unsupported file format: {ext}")
        raise ValueError(f"Unsupported file format: {ext}")
    SAVERS[ext](data, output_path, **kwargs)
