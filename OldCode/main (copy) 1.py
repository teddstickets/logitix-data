from flask import Flask, request, jsonify, render_template, url_for
import paramiko
import pandas as pd
import psycopg2
import requests
from io import StringIO
from datetime import datetime
from stat import S_ISDIR
import os

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


def fetch_csv_data(data, from_date_str=None, max_rows=None):
    HOST = data['host']
    PORT = int(data.get('port', 22))
    USERNAME = data['username']
    PASSWORD = data['password']
    FOLDER = data['folder']
    FILENAME = data['filename']

    transport = paramiko.Transport((HOST, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    full_path = f"{FOLDER}/{FILENAME}"
    with sftp.file(full_path, 'r') as file:
        content = file.read().decode()

    sftp.close()
    transport.close()

    df = pd.read_csv(StringIO(content))
    df['ticketgroup_create_datetime'] = pd.to_datetime(
        df['ticketgroup_create_datetime'], errors='coerce')

    if from_date_str:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
        df = df[df['ticketgroup_create_datetime'] >= from_date]

    if max_rows:
        max_rows = int(max_rows)
        df = df.head(max_rows)

    return df


@app.route('/fetch-csv', methods=['POST'])
def fetch_csv():
    try:
        data = request.json
        from_date_str = request.args.get('from_date')
        max_rows = request.args.get('rows')

        df = fetch_csv_data(data, from_date_str, max_rows)
        return df.to_json(orient='records')

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/insert-csv', methods=['POST'])
def insert_csv():
    try:
        data = request.json
        from_date_str = request.args.get('from_date')
        max_rows = request.args.get('rows')

        df = fetch_csv_data(data, from_date_str, max_rows)

        csvupdatename = f"{data['folder']}/{data['filename']}"
        csvupdatedate = datetime.utcnow()

        df['csvupdatename'] = csvupdatename
        df['csvupdatedate'] = csvupdatedate

        db_config = {
            "host": "35.224.109.158",
            "database": "ttDB",
            "user": "tedds-tickets-db",
            "password": "Autum2024"
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        df.columns = [col.strip() for col in df.columns]
        df = df.where(pd.notnull(df), None)
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})

        cast_to_int = [
            "TICKETGROUP_ID", "PRIMARY_PERFORMER_ID", "SECONDARY_PERFORMER_ID",
            "INVOICE_ID", "PO_ID", "INVOICE_NUMBER", "ENTITY_ID", "VENUE_ID",
            "PRODUCTION_ID", "CLIENT_CONSIGNMENT_ROLE", "QUANTITY",
            "PO_NUMBER", "BUDGET_ITEM_ID", "DAYS_OUT_NUMBER"
        ]
        for col in cast_to_int:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(float(x))
                                        if x is not None else None)

        df.columns = [col.upper() for col in df.columns]

        columns = list(df.columns)
        colnames = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f'INSERT INTO tickets.logitix_daily ({colnames}) VALUES ({placeholders})'

        rows = df.values.tolist()
        success_count = 0
        for row_num, row in enumerate(rows):
            try:
                cursor.execute(insert_sql, row)
                success_count += 1
            except Exception as e:
                print(f"\n🚨 ERROR ON ROW {row_num}:")
                for i, value in enumerate(row):
                    print(
                        f" - Column: {columns[i]} | Value: {value} | Type: {type(value)}"
                    )
                print("❌ SQL:", insert_sql)
                print("❌ Full error:", e)
                continue

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "status": "success",
            "inserted_rows": success_count,
            "skipped_rows": len(rows) - success_count
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/logitixdailyautoinsert', methods=['POST'])
def logitixdailyautoinsert():
    try:
        data = request.json
        HOST = data['host']
        PORT = int(data.get('port', 22))
        USERNAME = data['username']
        PASSWORD = data['password']
        BASE_FOLDER = data['base_folder']  # e.g., "/export_files"

        # Get today's folder name like 20250402
        today_folder = datetime.utcnow().strftime('%Y%m%d')
        full_folder_path = os.path.join(BASE_FOLDER, today_folder)

        # Connect to SFTP
        transport = paramiko.Transport((HOST, PORT))
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            files = sftp.listdir(full_folder_path)
        except FileNotFoundError:
            return jsonify({
                "status": "error",
                "message": f"Folder {full_folder_path} not found"
            }), 404

        csv_files = [f for f in files if f.endswith('.csv')]
        if not csv_files:
            return jsonify({
                "status":
                "error",
                "message":
                f"No CSV files found in folder {full_folder_path}"
            }), 404

        latest_csv = sorted(csv_files)[-1]

        # Prepare data dict for reuse
        data.update({'folder': full_folder_path, 'filename': latest_csv})

        df = fetch_csv_data(data)
        df['csvupdatename'] = f"{full_folder_path}/{latest_csv}"
        df['csvupdatedate'] = datetime.utcnow()

        df.columns = [col.strip() for col in df.columns]
        df = df.where(pd.notnull(df), None)
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})

        cast_to_int = [
            "TICKETGROUP_ID", "PRIMARY_PERFORMER_ID", "SECONDARY_PERFORMER_ID",
            "INVOICE_ID", "PO_ID", "INVOICE_NUMBER", "ENTITY_ID", "VENUE_ID",
            "PRODUCTION_ID", "CLIENT_CONSIGNMENT_ROLE", "QUANTITY",
            "PO_NUMBER", "BUDGET_ITEM_ID", "DAYS_OUT_NUMBER"
        ]
        for col in cast_to_int:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(float(x))
                                        if x is not None else None)

        df.columns = [col.upper() for col in df.columns]

        db_config = {
            "host": "35.224.109.158",
            "database": "ttDB",
            "user": "tedds-tickets-db",
            "password": "Autum2024"
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        columns = list(df.columns)
        colnames = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f'INSERT INTO tickets.logitix_daily ({colnames}) VALUES ({placeholders})'

        rows = df.values.tolist()
        success_count = 0
        for row_num, row in enumerate(rows):
            try:
                cursor.execute(insert_sql, row)
                success_count += 1
            except Exception as e:
                print(f"❌ Row {row_num} error:", e)
                continue

        conn.commit()
        cursor.close()
        conn.close()
        sftp.close()
        transport.close()

        return jsonify({
            "status": "success",
            "inserted_rows": success_count,
            "skipped_rows": len(rows) - success_count,
            "csv": latest_csv,
            "folder": full_folder_path
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/query-db', methods=['GET'])
def query_db():
    try:
        db_config = {
            "host": "35.224.109.158",
            "database": "ttDB",
            "user": "tedds-tickets-db",
            "password": "Autum2024"
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tickets.logitix_daily LIMIT 10;")

        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(colnames, row)) for row in rows]

        cursor.close()
        conn.close()

        return jsonify({"status": "success", "rows": results})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/get-my-ip', methods=['GET'])
def get_my_ip():
    try:
        ip = requests.get('https://api.ipify.org').text
        return jsonify({"your_public_ip": ip})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/hello', methods=['GET', 'POST'])
def hello_world():
    if request.method == 'POST':
        data = request.get_json()
        name = data.get('name', 'World')
    else:
        name = request.args.get('name', 'World')

    return jsonify({"message": f"Hello, {name}!"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
