from main import Transaction
from abc import ABC, abstractclassmethod
from enum import Enum

class FiledType(Enum):
  AND = "AND"
  OR = "OR"

class QueryConditon(ABC):
  """ クエリの条件式"""

  @abstractclassmethod
  def get_condition(self):
    pass

class ConditionPart(QueryConditon):

  def __init__(self):
    self._filed_type = None

  def where(self, filed_name, eval, value):
    self._fild_name = filed_name
    self._eval = eval
    self._value = value
    self._filed_type= FiledType.AND

  def or_where(self, filed_name, eval, value):
    self._fild_name = filed_name
    self._eval = eval
    self._value = value
    self._filed_type= FiledType.OR

  def get_condition(self):
    return f"{self._fild_name} {self._eval} {_define_value_type(self._value)}", self._filed_type, [self._value]

class ConditionGroup(QueryConditon):
  def __init__(self, condition_type, root_condition=False):
    self._conditions = []
    self._values = []
    self._condition_type = condition_type
    self._root_condition = root_condition

  def add(self, condition):
    self._conditions.append(condition)
  
  def get_condition(self):
    
    if len(self._conditions) == 0:
      raise Exception("conditionが存在しません")
    
    ret_condition = ""
    for index, condition in enumerate(self._conditions):
      conditon_str, conditon_type, condition_value = condition.get_condition()
      if index != 0:
        ret_condition += f' {conditon_type.value} '
      ret_condition += f' {conditon_str} '
      self._values.extend(condition_value)
    if not self._root_condition and len(self._conditions) > 1:
      ret_condition = f"({ret_condition}) "
    return ret_condition, self._condition_type, self._values
  
  def exists(self):
    return len(self._conditions) != 0

import re

class QueryBuilder(object):
  """ QueryBuilder"""

  def __init__(self, db_conecter=None, tx=None):
    self._table_name = None
    self._conditions = ConditionGroup(FiledType.AND, True)
    if tx is None and db_conecter is None:
      raise Exception("トランザクションかDBのコネクタは必須です")
    elif tx is not None and db_conecter is not None:
      raise Exception("トランザクションかDBコネクタはどちらかのみを設定してください")
    self._db_conecter = db_conecter
    self._tx = tx
    self._query_result = None
  
  def table(self, table_name, schema=None):
    """ テーブル名の指定を行う

    Args:
        table_name (str): テーブル名
    """
    self._table_name = table_name
    if self._tx is None and schema is not None:
      raise Exception("スキーマはトランザクションごとに指定してください")
    self._schema = schema
    return self

  def where(self, filed_name: str, eval: str, value):
    """ Where句用のビルダ(AND条件)

    Args:
        filed_name (str): フィールド名
        eval (str): 評価式
        value (any): 値

    Returns:
        this: 自身を返却する
    """
    condition_part = ConditionPart()
    condition_part.where(filed_name, eval, value)
    condition_group = ConditionGroup(FiledType.AND)
    condition_group.add(condition_part)
    self._conditions.add(condition_group)
    return self

  def or_where(self, filed_name, eval, value):
    """ Where句用のビルダ(OR条件)

    Args:
        filed_name (str): フィールド名
        eval (str): 評価式
        value (any): 値

    Returns:
        this: 自身を返却する
    """
    condition_part = ConditionPart()
    condition_part.or_where(filed_name, eval, value)
    condition_group = ConditionGroup(FiledType.OR)
    condition_group.add(condition_part)
    self._conditions.add(condition_group)
    return self

  def select(self, *args):
    """ SELECT文を実行する(全件取得)

    Returns:
        dict: 取得結果
    """
    query, query_values = self._query_build(self.__create_select_clause(*args))
    self._query_result = query

    if self._tx is not None:
      return self.__find_all(query, query_values, self._tx)
    else:
      with Transaction(self._db_conecter, True, self._schema) as tx:
        return self.__find_all(query, query_values, tx)

  def select_one(self, *args):
    query, query_values = self._query_build(self.__create_select_clause(*args))
    self._query_result = query
    if self._tx is not None:
      return self.__find_one(query, query_values, self._tx)
    else:
      with Transaction(self._db_conecter, True, self._schema) as tx:
        return self.__find_one(query, query_values, tx)

  def __create_select_clause(self, *args):
    select_filed = "*"
    if len(args) != 0:
      select_filed = ', '.join(args)
    return f"SELECT {select_filed} FROM {self._table_name}"

  def __find_all(self, query, query_values, tx):
    query = self._crean_query(query)
    if len(query_values) != 0:
      return tx.find_all(query, tuple(query_values))
    else:
      return tx.find_all(query)

  def __find_one(self, query, query_values, tx):
    query = self._crean_query(query)
    if len(query_values) != 0:
      return tx.find_one(query, tuple(query_values))
    else:
      return tx.find_one(query)

  def update(self, update_dict):
    if update_dict is None or not any(update_dict):
      raise Exception("アップデートのキーが存在しません")
    before_query = f"UPDATE {self._table_name} SET "
    update_values = []
    for index, key in enumerate(update_dict.keys()):
      if index != 0:
        before_query += ", "
      before_query += f"{key} = {_define_value_type(update_dict[key])}"
      update_values.append(update_dict[key])
    query, query_values = self._query_build(before_query)
    update_values.extend(query_values)
    query = self._crean_query(query)
    self._query_result = query
    if self._tx is not None:
      self._tx.save(query, tuple(update_values))
    else:
      with Transaction(self._db_conecter, True, self._schema) as tx:
        tx.save(query, tuple(update_values))

  def delete(self):
    query, query_values = self._query_build(f"DELETE FROM {self._table_name} ")
    query = self._crean_query(query)
    self._query_result = query
    if self._tx is not None:
      self._tx.save(query, tuple(query_values))
    else:
      with Transaction(self._db_conecter, True, self._schema) as tx:
        tx.save(query, tuple(query_values))

  def insert(self, insert_dict):
    if self._table_name is None:
      raise Exception("テーブルが指定されていません")
    if insert_dict is None or not any(insert_dict):
      raise Exception("アップデートのキーが存在しません")
    insert_values = []
    key_query = ""
    value_query = ""
    for index, key in enumerate(insert_dict.keys()):
      if index != 0:
        key_query += ", "
        value_query += ", "
      key_query += key
      value_query += _define_value_type(insert_dict[key])
      insert_values.append(insert_dict[key])
    query = f"INSERT INTO {self._table_name} ({key_query}) VALUES ({value_query})"
    query = self._crean_query(query)
    self._query_result = query
    if self._tx is not None:
      self._tx.save(query, tuple(insert_values))
    else:
      with Transaction(self._db_conecter, True, self._schema) as tx:
        tx.save(query, tuple(insert_values))

  def get_query_log(self):
    """実行したクエリの結果を返却する
    """
    if self._query_result is None:
      return "実行したクエリはありません"
    return self._query_result

  def _query_build(self, before_query="SELECT * FROM"):
    if self._table_name is None:
      raise Exception("テーブルが指定されていません")

    query = f"{before_query}"
    query_values = []
    if self._conditions.exists():
      query_str, _, query_values = self._conditions.get_condition()
      query += f" WHERE {query_str}"
    return query, query_values
  
  def _crean_query(self, query):
    query = re.sub('[ 　]+', ' ', query)
    query = re.sub(' $','', query)
    query += ';'
    return query

def _define_value_type(value):
  """ 値のタイプをもとにDBの値を返却する

  Args:
      value (any): 値を返却する
  """
  if type(value) is str or type(value) is int:
    return "%s"
  elif type(value) is dict or type(value) is list:
    return "%s::json"
  else:
    return "%s"
