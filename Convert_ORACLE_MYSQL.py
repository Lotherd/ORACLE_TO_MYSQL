import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import cx_Oracle
import mysql.connector
import sys
import os
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = collect_submodules('mysql.connector')

try:
    if sys.platform.startswith("darwin"):
        lib_dir = os.path.join(os.environ.get("HOME"), "Downloads", "instantclient_19_8")
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    elif sys.platform.startswith("win32"):
        lib_dir = r"instantclient_21_10"
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
except Exception as err:
    print("Whoops!")
    print(err)
    sys.exit(1)


# Define oracle_conn and mydb as global variables so that they can be accessed across functions
oracle_conn = None
mydb = None


def fetch_and_set():
    table_name_var.set('')
    table_listbox.delete(0, tk.END)
    table_listbox.insert(tk.END, *fetch_table_names())


# Function to establish connections with Oracle and MySQL databases
def connect_databases():
    global oracle_conn, mydb
    oracle_conn = cx_Oracle.connect(oracle_entry.get())
    mydb = mysql.connector.connect(**eval(mysql_entry.get()))
    messagebox.showinfo("Success", "Connected successfully.")


# Function to fetch table names
def fetch_table_names():
    global oracle_conn
    oracle_cur = oracle_conn.cursor()
    oracle_cur.execute("SELECT table_name FROM user_tables ORDER BY table_name ASC")
    table_names = [row[0] for row in oracle_cur]
    oracle_cur.close()
    return table_names


# Function to convert Oracle SQL tables into MySQL
def convert_tables():
    global oracle_conn, mydb
    selected_tables = table_listbox.curselection()  # Get the indices of selected tables
    selected_table_names = [table_listbox.get(idx) for idx in selected_tables]  # Get the table names using the indices

    for table_name in selected_table_names:
        oracle_cur = oracle_conn.cursor()
        mysql_cur = mydb.cursor()

        # Rest of the code for table conversion
        oracle_cur.execute(f"""
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = '{table_name}'
            ORDER BY column_id
        """)

        # Build CREATE TABLE command for MySQL
        create_table_sql = f'CREATE TABLE {table_name} (\n'
        for row in oracle_cur:
            if row[1] == 'NUMBER':
                if row[4] is not None:
                    column_def = '`{}` DECIMAL({}, {})'.format(row[0], row[3], row[4])
                else:
                    column_def = '`{}` INT'.format(row[0])
            elif row[1] == 'VARCHAR2' or row[1] == 'VARCHAR' or row[1] == 'NVARCHAR2':
                column_def = '`{}` VARCHAR({})'.format(row[0], row[2])
            elif row[1] == 'DATE':
                column_def = '`{}` DATE'.format(row[0])
            elif row[1] == 'XMLTYPE':
                column_def = '`{}` TEXT'.format(row[0])
            elif row[1] == 'CLOB':
                column_def = '`{}` TEXT'.format(row[0])
            elif row[1] == 'BLOB':
                column_def = '`{}` BLOB'.format(row[0])
            else:
                column_def = '`{}` {}'.format(row[0], row[1])

            if row[5] == 'N':
                column_def += ' NOT NULL'

            create_table_sql += '  ' + column_def + ',\n'

        # Create table in MySQL
        mysql_cur.execute(create_table_sql.rstrip(',\n') + '\n);')

        # Read Oracle indexes
        oracle_cur.execute(f"""
            SELECT index_name, column_name
            FROM user_ind_columns
            WHERE table_name = '{table_name}'
        """)
        # Build CREATE INDEX commands for MySQL
        for row in oracle_cur:
            # Add prefix length for varchar and similar fields
            index_field = f"`{row[1]}`(50)" if row[1] in ["VARCHAR", "VARCHAR2", "XMLTYPE"] else f"`{row[1]}`"
            create_index_sql = 'CREATE INDEX `{}` ON `{}` ({});'.format(row[0], table_name, index_field)
            try:
                mysql_cur.execute(create_index_sql)
            except mysql.connector.errors.ProgrammingError as err:
                if err.errno == 1061:  # If it's a duplicate key error, skip to the next
                    continue
                else:
                    raise
# Read Oracle constraints
        oracle_cur.execute(f"""
        SELECT a.constraint_name, a.constraint_type, a.table_name, LISTAGG(c.column_name, ',') WITHIN GROUP (ORDER BY c.position) as columns, 
            a.r_constraint_name
        FROM all_constraints a
        JOIN all_cons_columns c
        ON a.constraint_name = c.constraint_name
        WHERE a.table_name = '{table_name}'
        GROUP BY a.constraint_name, a.constraint_type, a.table_name, a.r_constraint_name
        """)

        # ...
        # Prepare dictionary for foreign key reference table and column
        fk_info = {}

        oracle_cur_fk = oracle_cur.connection.cursor()
        oracle_cur_fk.execute(f"""
        SELECT a.constraint_name, c.table_name, c.column_name
        FROM all_constraints a
        INNER JOIN all_cons_columns c
        ON a.constraint_name = c.constraint_name
        WHERE a.constraint_type = 'P'
        """)

        for row in oracle_cur_fk:
            fk_info[row[0]] = (row[1], row[2])  # storing table name and columns as a tuple against each constraint name

        oracle_cur_fk.close()

        # Build ALTER TABLE commands for MySQL
        for row in oracle_cur:
            try:
                if row[1] == 'P':
                    alter_table_sql = f'ALTER TABLE {table_name} ADD PRIMARY KEY ({row[3]});'
                elif row[1] == 'R':
                    # Use the foreign key info
                    fk_table, fk_column =  fk_info[row[4]] # using r_constraint_name to find the PK constraint it references
                    alter_table_sql = f'ALTER TABLE {table_name} ADD FOREIGN KEY ({row[3]}) REFERENCES {fk_table}({fk_column});'
                else:
                    continue  # skip other constraint types (including check constraints)

                mysql_cur.execute(alter_table_sql)
            except mysql.connector.Error as err:
                print(f"Error: {err}")
                print(f"Skipped constraint creation for {row[0]}")

        # Read Oracle triggers
        oracle_cur.execute(f"""
            SELECT trigger_name, trigger_type, triggering_event, table_name, trigger_body
            FROM user_triggers
            WHERE table_name = '{table_name}'
        """)

        # Iterate through each trigger
        for row in oracle_cur:
            trigger_name, trigger_type, triggering_event, table_name, trigger_body = row

            # Convert trigger type to MySQL equivalent (assuming all are row-level)
            trigger_parts = trigger_type.split()
            trigger_timing = trigger_parts[0]  # BEFORE/AFTER
            trigger_event = trigger_parts[1]  # INSERT/UPDATE/DELETE
            trigger_timing = trigger_timing.replace("BEFORE", "BEFORE").replace("AFTER", "AFTER")  # MySQL supports the same BEFORE and AFTER keywords
            trigger_event = trigger_event.replace("INSERT", "INSERT").replace("UPDATE", "UPDATE").replace("DELETE", "DELETE")  # MySQL supports the same INSERT, UPDATE, and DELETE keywords

            # Convert trigger body (naive conversion - may require manual adjustments)
            trigger_body = trigger_body.replace(":", "NEW.")  # Convert Oracle's :NEW to MySQL's NEW.
            trigger_body = trigger_body.replace("OLD.", "OLD.")  # Convert Oracle's :OLD to MySQL's OLD.

            # Generate MySQL CREATE TRIGGER statement
            create_trigger_sql = f"""
                CREATE TRIGGER {trigger_name}
                {trigger_timing} {trigger_event}
                ON {table_name}
                FOR EACH ROW
                {trigger_body}
            """

            # Create trigger in MySQL
            try:
                mysql_cur.execute(create_trigger_sql)
            except mysql.connector.Error as err:
                print(f"Error: {err}")
                print(f"Skipped trigger creation for {trigger_name}")

        # Close Oracle cursor
        oracle_cur.close()

    # Close MySQL cursor and commit changes
    mysql_cur.close()
    mydb.commit()

    messagebox.showinfo("Success", "Table conversion completed successfully.")


# Function to extract metadata for the selected tables
def extract_metadata():
    global oracle_conn
    selected_tables = table_listbox.curselection()  # Get the indices of selected tables
    selected_table_names = [table_listbox.get(idx) for idx in selected_tables]  # Get the table names using the indices

    with open('metadata.txt', 'w') as file:
        for table_name in selected_table_names:
            oracle_cur = oracle_conn.cursor()
            oracle_cur.execute(f"""
                SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM user_tab_columns
                WHERE table_name = '{table_name}'
                ORDER BY column_id
            """)

            # Write metadata for the table to the file
            file.write(f"Metadata for table: {table_name}\n")
            for row in oracle_cur:
                column_name, data_type, data_length, data_precision, data_scale, nullable = row
                file.write(f"Column: {column_name}, Type: {data_type}, Length: {data_length}, Precision: {data_precision}, "
                           f"Scale: {data_scale}, Nullable: {nullable}\n")

            # Close Oracle cursor
            oracle_cur.close()

            file.write('\n')  # Add a newline after each table's metadata

    messagebox.showinfo("Success", "Metadata extraction completed successfully.")


# GUI
master = tk.Tk()
master.title("Oracle to MySQL Converter")

style = ttk.Style()
style.configure("TLabel", font=("Arial", 11))
style.configure("TButton", font=("Arial", 11))
style.configure("TEntry", font=("Arial", 11))
style.configure("TCombobox", font=("Arial", 11))

frame1 = ttk.Frame(master, padding="10")
frame1.grid(row=0, column=0, sticky=(tk.E, tk.W))

frame2 = ttk.Frame(master, padding="10")
frame2.grid(row=1, column=0, sticky=(tk.E, tk.W))

ttk.Label(frame1, text="Oracle connection (username/password@hostname:port/service_name):").grid(row=0, column=0, sticky=tk.W)
oracle_entry = ttk.Entry(frame1, width=200)
oracle_entry.grid(row=1, column=0, padx=(0, 10))

ttk.Label(frame2, text="MySQL connection (dict as a string: {'host': 'hostname', 'user': 'username', 'password': 'password', 'database': 'database'}):").grid(row=0, column=0, sticky=tk.W)
mysql_entry = ttk.Entry(frame2, width=200)
mysql_entry.grid(row=1, column=0, padx=(0, 10))

ttk.Button(master, text='Connect', command=connect_databases).grid(row=2, column=0, sticky=(tk.E, tk.W), pady=(10, 0))

ttk.Button(master, text='Fetch Tables', command=fetch_and_set).grid(row=3, column=0, sticky=(tk.E, tk.W), pady=10)

table_name_var = tk.StringVar(master)
ttk.Label(master, text="Oracle Table Names:").grid(row=4, column=0, sticky=tk.W)
table_listbox = tk.Listbox(master, selectmode=tk.MULTIPLE, width=70)
table_listbox.grid(row=5, column=0, sticky=(tk.E, tk.W), padx=(0, 10))

ttk.Button(master, text='Convert', command=convert_tables).grid(row=6, column=0, sticky=(tk.E, tk.W), pady=10)
ttk.Button(master, text='Extract Metadata', command=extract_metadata).grid(row=7, column=0, sticky=(tk.E, tk.W), pady=10)

master.columnconfigure(0, weight=1)  # This makes the column expand to fill any extra space

master.mainloop()
