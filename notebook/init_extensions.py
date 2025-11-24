import duckdb


EXT_DIR = "/opt/duckdb/extensions"

con = duckdb.connect()
con.execute(f"SET extension_directory='{EXT_DIR}'")
con.execute("INSTALL httpfs;")
con.execute("INSTALL ducklake;")
con.execute("INSTALL postgres;")

con.close()

