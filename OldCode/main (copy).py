from flask import Flask, request, jsonify
import paramiko
import pandas as pd
import psycopg2
import requests
from io import StringIO
from datetime import datetime

app = Flask(__name__)

@app.route('/hello', methods=['GET', 'POST'])
def hello_world():
    if request.method == 'POST':
        data = request.get_json()
        name = data.get('name', 'World')
    else:
        name = request.args.get('name', 'World')

    return jsonify({"message": f"Hello, {name}!"})

@app.route('/get-csv', methods=['POST'])
def get_csv():
    data = request.json
    HOST = data['host']
    PORT = int(data.get('port', 22))
    USERNAME = data['username']
    PASSWORD = data['password']
    FOLDER = data['folder']
    FILENAME = data['filename']

    max_rows = request.args.get('rows')
    from_date_str = request.args.get('from_date')  # expects format: YYYY-MM-DD

    try:
        transport = paramiko.Transport((HOST, PORT))
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        full_path = f"{FOLDER}/{FILENAME}"
        with sftp.file(full_path, 'r') as file:
            content = file.read().decode()

        sftp.close()
        transport.close()

        df = pd.read_csv(StringIO(content))
        df['ticketgroup_create_datetime'] = pd.to_datetime(df['ticketgroup_create_datetime'], errors='coerce')

        if from_date_str:
            try:
                from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
                df = df[df['ticketgroup_create_datetime'] >= from_date]
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}), 400

        base_cols = ["primary_performer", "event_date", "venue", "region_abbr", "customer_channel", "quantity", "total_cost"]
        df_selected = df[base_cols]

        if max_rows:
            try:
                max_rows = int(max_rows)
                df_selected = df_selected.head(max_rows)
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid rows parameter. It must be an integer."}), 400

        return jsonify({"status": "success", "rows": df_selected.to_dict(orient="records")})

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
        cursor.execute("SELECT * FROM public.logitix_20250101 LIMIT 10;")

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)  # Replit uses port 80 for external access
