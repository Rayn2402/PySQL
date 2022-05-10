"""
    @Author:                    Nicolas Raymond
    @Creation date:             2022/04/10
    @Description: This file stores the DataManager class that is used to interact with a PostgreSQL database
"""

import psycopg2
import os

from pandas import DataFrame
from typing import Any, Dict, Final, List, Optional, Tuple


class DataManager:
    """
    Object that interacts with a PostgresSQL database
    """
    # Results dictionary keys
    VAR_NAME: Final[str] = "Variable Name"
    ALL: Final[str] = "All"

    # Temporary table name
    TEMP: Final[str] = "temp"

    def __init__(self,
                 user: str,
                 password: str,
                 database: str,
                 host: str = 'localhost',
                 port: str = '5432',
                 schema: str = 'public'):
        """
        Sets the private attributes
        Args:
            user: username to access the database
            password: password linked to the user
            database: name of the database
            host: database host address
            port: connection port number
            schema: schema we want to access in the database
        """
        self.__conn = DataManager._connect(user, password, database, host, port)
        self.__schema = schema
        self.__database = database

    def _create_table(self,
                      table_name: str,
                      types: Dict[str, str],
                      primary_key: Optional[List[str]] = None) -> None:
        """
        Creates a table named "table_name" that has the columns with the types indicated
        in the dictionary "types".

        Args:
            table_name: name of the new table
            types: dictionary with the names of the columns (keys) and their respective types (values)
            primary_key: list of column names to use in order to build the primary key

        Returns: None
        """
        # We save the start of the query
        query = f"CREATE TABLE {self.__schema}.\"{table_name}\" (" + self._reformat_columns_and_types(types)

        # If a primary key is given we add it to the query
        if primary_key is not None:

            # We define the primary key
            keys = self._reformat_columns(primary_key)
            query += f", PRIMARY KEY ({keys}) );"

        else:
            query += ");"

        # We execute the query
        with self.__conn.cursor() as curs:
            try:
                curs.execute(query)
                self.__conn.commit()

            except psycopg2.Error as e:
                raise Exception(e.pgerror)

    def create_and_fill_table(self,
                              df: DataFrame,
                              table_name: str,
                              types: Dict[str, str],
                              primary_key: Optional[List[str]] = None) -> None:
        """
        Creates a new table and fills it using the data from the a pandas dataframe

        Args:
            df: pandas dataframe
            table_name: name of the new table
            types: dictionary with the names of the columns (keys) and their respective types (values)
            primary_key: list of column names to use in order to build the primary key

        Returns: None
        """
        # We first create the table
        self._create_table(table_name, types, primary_key)

        # We order columns of dataframe according to "types" dictionary
        df = df[types.keys()]

        # We save the dataframe in a temporary csv
        df.to_csv(DataManager.TEMP, index=False, na_rep=" ", sep="!")

        # We copy the data from the csv into the table
        file = open(DataManager.TEMP, mode="r", newline="\n")
        file.readline()

        # We copy the data to the table
        with self.__conn.cursor() as curs:
            try:
                curs.copy_from(file, f"{self.__schema}.\"{table_name}\"", sep="!", null=" ")
                self.__conn.commit()
                os.remove(DataManager.TEMP)

            except psycopg2.Error as e:
                os.remove(DataManager.TEMP)
                raise Exception(e.pgerror)

    def get_column_names(self, table_name: str) -> List[str]:
        """
        Retrieves the names of all the columns in a given table

        Args:
            table_name: name of the table

        Returns: list of column names
        """
        table_name = table_name.replace("'", "''")
        with self.__conn.cursor() as curs:
            try:
                curs.execute(
                    f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= \'{table_name}\'")

                # We extract the columns' names
                columns_names = list(map(lambda c: c[0], curs.fetchall()))
            except psycopg2.Error as e:
                raise Exception(e.pgerror)

        return columns_names

    def get_table(self,
                  table_name: str,
                  columns: Optional[List[str]] = None) -> DataFrame:
        """
        Retrieves a table from the database

        Args:
            table_name: name of the table
            columns: list of the columns we want to select (default = None (all columns))

        Returns: pandas dataframe
        """

        # If no column names are specified, we select all columns
        query = "SELECT *" if columns is None else f"SELECT {self._reformat_columns(columns)} "

        # We add the table name to the query
        query = f"{query} FROM {self.__schema}.\"{table_name}\""

        with self.__conn.cursor() as curs:
            try:
                # We execute the query
                curs.execute(query)

                # We retrieve the column names and the data
                columns = [desc[0] for desc in curs.description]
                data = curs.fetchall()

            except psycopg2.Error as e:
                raise Exception(e.pgerror)

        # We create a pandas dataframe
        df = DataFrame(data=data, columns=columns)

        return df

    @staticmethod
    def _connect(user: str,
                 password: str,
                 database: str,
                 host: str,
                 port: str) -> Any:
        """
        Creates a connection with a database
        Args:
            user: username to access the database
            password: password linked to the user
            database: name of the database
            host: database host address
            port: connection port number
        Returns: connection, cursor
        """
        try:
            conn = psycopg2.connect(database=database,
                                    user=user,
                                    host=host,
                                    password=password,
                                    port=port)

        except psycopg2.Error as e:
            raise Exception(e.pgerror)

        return conn

    @staticmethod
    def _initialize_results_dict(df: DataFrame,
                                 group: str) -> Tuple[Dict[str, List[Any]], List[Any]]:
        """
        Initializes a dictionary that will contain the results of a descriptive analyses

        Args:
            df: pandas dataframe
            group: name of a column

        Returns: initialized dictionary, list with the different group values
        """
        group_values = None
        results = {
            DataManager.VAR_NAME: [],
            DataManager.ALL: []
        }
        if group is not None:
            group_values = df[group].unique()
            for group_val in group_values:
                results[f"{group} {group_val}"] = []

        return results, group_values

    @staticmethod
    def _reformat_columns(cols: List[str]) -> str:
        """
        Converts a list of strings containing the name of columns to a string
        ready to use in an SQL query. Ex: ["name","age","gender"]  ==> "name","age","gender"

        Args:
            cols: list of column names

        Returns: str
        """
        cols = list(map(lambda c: '"' + c + '"', cols))

        return ",".join(cols)

    @staticmethod
    def _reformat_columns_and_types(types: Dict[str, str]) -> str:
        """
        Converts a dictionary with column names as keys and types as values to a string to
        use in an SQL query. Ex: {"name":"text", "Age":"numeric"} ==> '"name" text, "Age" numeric'

        Args:
            types: dictionary with column names (keys) and types (values)

        Returns: str
        """
        cols = types.keys()
        query_parts = list(map(lambda c: f"\"{c}\" {types[c]}", cols))

        return ",".join(query_parts)

    @staticmethod
    def get_categorical_var_analysis(df: DataFrame,
                                     group: Optional[str] = None) -> DataFrame:
        """
        Calculates the counts and percentages of represent by modalities of all the columns (except "group" column)
        in the dataframe. These statistics are retrieved over all rows and also over groups contained
        in the specified "group" column.

        Args:
            df: pandas dataframe with the categorical variables
            group: name of a column (Ex : group = "Sex" will give us the stats for all the data, for men  and for women)

        Returns: pandas dataframe
        """

        # We initialize a python dictionary in which we will save the results
        results, group_values = DataManager._initialize_results_dict(df, group)

        # We get the columns on which we will calculate the stats
        if group is not None:
            cols = [col for col in df.columns if col != group]
        else:
            cols = df.columns

        # We initialize a dictionary that will store the totals of each group within the column "group"
        group_totals = {}

        # For each column we calculate the count and the percentage
        for col in cols:

            # We get all the categories of this variable
            categories = df[col].dropna().unique()

            # We get the total count
            total = df.shape[0] - df[col].isna().sum()

            # For each category of this variable we get the counts and the percentage
            for category in categories:

                # We get the total count of this category
                category_total = df[df[col] == category].shape[0]

                # We save the results
                results[DataManager.VAR_NAME].append(f"{col} : {category}")
                results[DataManager.ALL].append(f"{category_total} ({category_total/total:.2%})")

                if group is not None:

                    for group_val in group_values:

                        # We get the number of elements within the group
                        group_totals[group_val] = df.loc[df[group] == group_val, col].dropna().shape[0]

                        # We create a filter to get the number of items in a group that has the correspond category
                        filter_ = (df[group] == group_val) & (df[col] == category)

                        # We compute the statistics needed
                        sub_category_total = df[filter_].shape[0]
                        sub_category_percent = f"{sub_category_total/group_totals[group_val]:.2%}"
                        results[f"{group} {group_val}"].append(f"{sub_category_total} ({sub_category_percent})")

        return DataFrame(results)

    @staticmethod
    def get_numerical_column_stats(df: DataFrame,
                                   col: str) -> Tuple[float, float, float, float]:
        """
        Retrieves statistic from a numerical column in a pandas dataframe

        Args:
            df: pandas dataframe
            col: name of the column

        Returns: mean, std, max, min
        """
        numerical_data = df[col].astype("float")
        mean = numerical_data.mean(axis=0)
        std = numerical_data.std(axis=0)
        min_ = numerical_data.min()
        max_ = numerical_data.max()

        return mean, std, min_, max_

    @staticmethod
    def get_numerical_var_analysis(df: DataFrame,
                                   group: Optional[str] = None) -> DataFrame:
        """
        Calculates the mean, the variance, the min and the max of variables
        within a given data frame over all rows, and also over groups contained in a specified column "group".
        Args:
            df: pandas data frame containing the data of numerical variables
            group: name of a column, Ex : group = "Sex" will give us the stats for all the data, for men, and for women
        Returns: pandas dataframe with the statistics
        """
        # We initialize a python dictionary in which we will save the results
        results, group_values = DataManager._initialize_results_dict(df, group)

        # We get the columns on which we will calculate the stats
        if group is not None:
            cols = [col for col in df.columns if col != group]
        else:
            cols = df.columns

        # For each column we calculate the mean and the variance
        for col in cols:

            # We append the statistics for all participants to the results dictionary
            results[DataManager.VAR_NAME].append(col)
            all_mean, all_var, all_min, all_max = DataManager.get_numerical_column_stats(df, col)
            results[DataManager.ALL].append(f"{all_mean:.2f} ({all_var:.2f}) [{all_min:.2f}, {all_max:.2f}]")

            # If a group column is given, we calculate the stats for each possible value of that group
            if group is not None:
                for group_val in group_values:

                    # We append the statistics for sub group participants to the results dictionary
                    df_group = df[df[group] == group_val]
                    group_mean, group_var, group_min, group_max = DataManager.get_numerical_column_stats(df_group, col)
                    stats = f"{group_mean:.2f} ({group_var:.2f}) [{group_min:.2f}, {group_max:.2f}]"
                    results[f"{group} {group_val}"].append(stats)

        return DataFrame(results)
