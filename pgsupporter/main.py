import psycopg2
import psycopg2.pool
from psycopg2.extras import DictCursor

class DbConnecter(object):
  """DBの接続情報をもつクラス"""
  def __init__(self, dbname: str, host: str, user: str, password: str, min_con=5, max_con=10, pool_flg=True):
    """ コンストラクタ

    Args:
        dbname (str): DB名
        host (str): 接続先ホスト名
        user (str): 接続先ユーザ名
        password (str): 接続ユーザパスワード
        pool_flg (bool, optional): コネクションプールを使用する場合 True / コネクションプールを使用しない場合 False. Defaults to True.
        min_con (int, optional): 最小のコネクションプール数. Defaults to 5.
        max_con (int, optional): 最大のコネクションプール数. Defaults to 10.
    """
    self.__setting(dbname, host, user, password, min_con, max_con, pool_flg)
  
  def __setting(self, dbname: str, host: str, user: str, password:str, min_con=5, max_con=10, pool_flg=True):
    self.__dbname = dbname
    self.__host = host
    self.__user = user
    self.__password = password
    self.__dsn = f"dbname={self.__dbname} host={self.__host} user={self.__user} password={self.__password}"
    self.__pool_flg = pool_flg
    if self.__pool_flg:
      self.__min_con = min_con
      self.__max_con = max_con
      self.__pool = psycopg2.pool.SimpleConnectionPool(dsn=self.__dsn, minconn=self.__min_con, maxconn=self.__max_con)
  
  def __str__(self):
    return f"DB名: {self.__dbname}, ホスト: {self.__host}, ユーザ: {self.__user}, パスワード: {self.__password}"

  def get_connect(self):
    """ コネクションの取得"""
    if self.__pool_flg:
      return self.__pool.getconn()
    return psycopg2.connect(self.__dsn)

_default_conection = None
def create_default_connection(dbname: str, host: str, user: str, password: str, min_con=5, max_con=10, pool_flg=True):
  """ デフォルトで使用するコネクタを設定

  Args:
      dbname (str): DB名
      host (str): 接続先ホスト名
      user (str): 接続先ユーザ名
      password (str): 接続ユーザパスワード
      pool_flg (bool, optional): コネクションプールを使用する場合 True / コネクションプールを使用しない場合 False. Defaults to True.
      min_con (int, optional): 最小のコネクションプール数. Defaults to 5.
      max_con (int, optional): 最大のコネクションプール数. Defaults to 10.
  """
  _default_conection = DbConnecter(dbname, host, user, password, min_con, max_con, pool_flg)

class Transaction(object):
  """ トランザクションを処理を実行するためのクラス
      connector: DBConnecterクラスを使用したDBへの接続情報
      read_only:  読み込み用のトランザクション: true(初期値) 登録用のトランザクション: false
      schema: schemaを指定することでトランザクションのスキーマを変更

      使用例) 
      ・DBからのデータ取得の場合
      with Transaction(connector, true) as tx
        results = tx.find_all(query)

      ・DBへのデータ登録の場合
      with Transaction(connector, false) as tx
        tx.save(query)

      ・DBからデータを削除の場合
      with Transaction(connector, false) as tx
        tx.delete(query)
  """
  def __init__(self, connector: DbConnecter, read_only=True, schema=None):
    """ コンストラクタ

    Args:
        connector (DbConnecter): コネクションの取得
        read_only (bool, optional): 読み込み専用の場合 True/ その他の場合 False. Defaults to True.
        schema (_type_, optional): スキーマ名. Defaults to None.
    """
    self.__connector = connector
    self.__read_only = read_only
    self.__schema = schema

  def open(self):
    """ トランザクションを開始する"""
    self.__connect = self.__connector.get_connect()

  def close(self, success_flg=True):
    """ トランザクションを終了する
        読み込みモードの場合は処理が成功したか否かでトランザクションをコミットするかロールバックするかを分ける
        success_flg: 処理成功の場合: true(初期値) 処理失敗の場合はfalse 
    """
    if not self.__read_only:
      if success_flg: 
        self.__connect.commit()
      else:
        self.__connect.rollback()
    self.__connect.close()

  def find_all(self, query, vars=None):
    """ 全件取得"""
    with self.__connect.cursor(cursor_factory = DictCursor) as cur:
      cur.execute(query, vars)
      result = cur.fetchall()
      return result
  
  def find_one(self, query, vars=None):
    """ 1件取得"""
    with self.__connect.cursor(cursor_factory = DictCursor) as cur:
      cur.execute(query, vars)
      result = cur.fetchone()
      return result

  def save(self, query, vars = None):
    """ 保存処理"""
    with self.__connect.cursor() as cur:
      cur.execute(query, vars)

  def delete(self, query, vars = None):
    """ 削除処理"""
    with self.__connect.cursor() as cur:
      cur.execute(query, vars)

  def change_schema(self, schema_name):
    """ スキーマの変更"""
    with self.__connect.cursor() as cur:
      cur.execute(f'SET search_path TO {schema_name},public;')

  def execute_ddl(self, query):
    """DDL実行用の処理
       NOTE: DDLの実行がトランザクションクラスで行えるのは微妙だと思うものの
       接続情報等を使いまわして使用できる方が利便性があるなと思い、ここで実行できるようにする

    Args:
        query (str): 実行するDDL
    """
    with self.__connect.cursor() as cur:
      cur.execute(query, vars)

  def __enter__(self):
    self.open()
    if self.__schema is not None:
      self.change_schema(self.__schema)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close(exc_type is None)
    raise Exception(f'トランザクションで例外が発生しました。エラーの種類: {exc_type}\nエラーの値: {exc_value}\n{traceback}')

def start_transaction(read_only=True, schema:str=None, connector: DbConnecter=None):
  """ トランザクションの開始

  Args:
      read_only (bool, optional): 読み取り専用の場合 True/ その他の場合 False. Defaults to True.
      schema (str, optional): _description_. Defaults to None.
      connector (DbConnecter, optional): _description_. Defaults to None.

  Returns:
      _type_: _description_
  """
  if connector is None and _default_conection is None:
    raise Exception("使用できるコネクタが存在しません。create_connectionにてデフォルトのコネクタを設定するか引数でconnectorを指定してください。")
  if connector is not None:
    return Transaction(connector, read_only, schema)
  return Transaction(_default_conection, read_only, schema)
