import os
import shutil
import zipfile
import tempfile
import subprocess
import threading
import time
import uuid
import logging
import psutil
import sys
import requests
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify, abort, send_file, session, redirect, url_for
import io
import json
from functools import wraps
from flask_socketio import SocketIO, emit


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ConsoleManager")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ultra_pro_secret_key_v2'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB max zip size
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INSTANCES_DIR = os.path.join(BASE_DIR, 'instances')
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads')
ACCOUNTS_FILE = os.path.join(BASE_DIR, 'accounts.json')

for d in [INSTANCES_DIR, UPLOAD_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

def load_accounts():
    if not os.path.exists(ACCOUNTS_FILE):
        return {}
    with open(ACCOUNTS_FILE, 'r') as f:
        return json.load(f)

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, 'w') as f:
        json.dump(accounts, f, indent=4)

# Instances Persistence Helpers
INSTANCES_FILE = os.path.join(BASE_DIR, 'instances.json')

def load_instances():
    if not os.path.exists(INSTANCES_FILE):
        return {}
    try:
        with open(INSTANCES_FILE, 'r') as f:
            data = json.load(f)
            loaded = {}
            for uid, info in data.items():
                loaded[uid] = {
                    'process': None,
                    'status': 'stopped',
                    'logs': [],
                    'path': info.get('path', os.path.join(INSTANCES_DIR, uid)),
                    'name': info.get('name', f"Instance {uid}"),
                    'owner': info.get('owner')
                }
            return loaded
    except Exception as e:
        logger.error(f"Failed to load instances from disk: {e}")
        return {}

def save_instances_persist():
    data = {}
    for uid, info in instances.items():
        data[uid] = {
            'path': info.get('path'),
            'name': info.get('name'),
            'owner': info.get('owner')
        }
    try:
        with open(INSTANCES_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"Failed to persist instances to disk: {e}")

# Auth Decorators
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'success': False, 'error': 'Unauthorized'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'role' not in session or session['role'] != 'admin':
            if request.path.startswith('/api/'):
                return jsonify({'success': False, 'error': 'Forbidden: Admin access required'}), 403
            return "Forbidden: Admin access required", 403
        return f(*args, **kwargs)
    return decorated_function

# State management
# { instance_id: { 'process': Popen, 'status': str, 'logs': list, 'path': str, 'name': str, 'uptime': float } }
instances = load_instances()

def get_directory_size_mb(path):
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except Exception:
        pass
    return round(total_size / (1024 * 1024), 2)

@app.route('/api/metrics', methods=['GET'])
@login_required
def get_metrics():
    """API Endpoint to fetch system metrics periodically."""
    try:
        # Global Metrics
        cpu_percent = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Extended global metrics
        cpu_freq = psutil.cpu_freq()
        cpu_speed = round(cpu_freq.current / 1000, 2) if cpu_freq else 0
        cpu_cores = psutil.cpu_count(logical=True)
        
        global_metrics = {
            'cpu': cpu_percent,
            'ram_used': round(ram.used / (1024 * 1024), 1),
            'ram_total': round(ram.total / (1024 * 1024), 1),
            'ram_percent': ram.percent,
            'disk_percent': disk.percent,
            'cpu_speed': cpu_speed,
            'cpu_cores': cpu_cores
        }

        # Instance Uptime & Process Metrics
        # Instance Uptime, Process Metrics & Folder Size
        uptime_data = {}
        process_metrics = {}
        folder_sizes = {}
        current_time = time.time()
        
        for uid, info in instances.items():
            folder_sizes[uid] = get_directory_size_mb(info['path'])
            
            if info['status'] == 'running':
                # Uptime
                if 'start_time' in info:
                    uptime_seconds = int(current_time - info['start_time'])
                    hrs = uptime_seconds // 3600
                    mins = (uptime_seconds % 3600) // 60
                    secs = uptime_seconds % 60
                    uptime_data[uid] = f"{hrs:02d}:{mins:02d}:{secs:02d}"
                    
                # Process CPU/RAM
                if info.get('process') and info['process'].poll() is None:
                    try:
                        p = psutil.Process(info['process'].pid)
                        # Process metrics
                        process_metrics[uid] = {
                            'cpu': round(p.cpu_percent(interval=None), 1),
                            'ram': round(p.memory_info().rss / (1024 * 1024), 1) # MB
                        }
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

        return jsonify({
            'success': True,
            'global': global_metrics,
            'uptimes': uptime_data,
            'process_metrics': process_metrics,
            'folder_sizes': folder_sizes
        })

    except Exception as e:
        logger.error(f"Metrics error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def stream_logs(instance_id, process):
    """Background thread to read subprocess logs and emit them."""
    try:
        # Read line by line as long as process outputs
        for line in iter(process.stdout.readline, b''):
            log_line = line.decode('utf-8', errors='replace').strip()
            if instance_id in instances:
                instances[instance_id]['logs'].append(log_line)
                # Keep only last 200 lines to prevent memory bloat
                if len(instances[instance_id]['logs']) > 200:
                   instances[instance_id]['logs'].pop(0) 
                   
                socketio.emit('log_update', {'id': instance_id, 'log': log_line}, namespace='/')
    except Exception as e:
        logger.error(f"Error streaming logs for {instance_id}: {e}")
    finally:
        try:
            process.stdout.close()
        except:
            pass
            
        if instance_id in instances:
            instances[instance_id]['status'] = 'stopped'
            socketio.emit('status_update', {'id': instance_id, 'status': 'stopped'}, namespace='/')
            logger.info(f"Process {instance_id} stopped.")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.json or request.form
        username = data.get('username')
        password = data.get('password')
        
        accounts = load_accounts()
        if username in accounts and accounts[username]['password'] == password:
            session['username'] = username
            session['role'] = accounts[username].get('role', 'user')
            return jsonify({'success': True}) if request.is_json else redirect(url_for('index'))
            
        return jsonify({'success': False, 'error': 'Invalid credentials'}) if request.is_json else "Invalid credentials", 401
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        data = request.json or request.form
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'Username and password required.'}), 400
            
        accounts = load_accounts()
        if username in accounts:
            return jsonify({'success': False, 'error': 'Username already exists.'}), 400
            
        settings = accounts.get('__settings__', {})
        default_limit = settings.get('default_max_instances', 1)
            
        accounts[username] = {
            'password': password,
            'role': 'user',
            'max_instances': max(default_limit, 1)
        }
        save_accounts(accounts)
        
        # Auto-login
        session['username'] = username
        session['role'] = 'user'
        
        return jsonify({'success': True}) if request.is_json else redirect(url_for('index'))
        
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    return render_template('index.html', user=session['username'], role=session['role'])

@app.route('/api/instances', methods=['GET'])
@login_required
def list_instances():
    data = []
    current_time = time.time()
    filter_type = request.args.get('filter', 'all')
    try:
        accounts = load_accounts()
    except:
        accounts = {}
        
    for uid, info in instances.items():
        owner = info.get('owner')
        owner_role = accounts.get(owner, {}).get('role', 'user') if owner else 'system'
        # Role-based filtering
        if session['role'] != 'admin':
            if owner != session['username']:
                continue
        else:
            if filter_type == 'mine' and owner_role != 'admin':
                continue
            if filter_type == 'users' and (owner_role == 'admin' or owner_role == 'paid user'):
                continue
            if filter_type == 'paid' and owner_role != 'paid user':
                continue
            
        uptime_str = "00:00:00"
        if info['status'] == 'running' and 'start_time' in info:
            uptime_seconds = int(current_time - info['start_time'])
            hrs = uptime_seconds // 3600
            mins = (uptime_seconds % 3600) // 60
            secs = uptime_seconds % 60
            uptime_str = f"{hrs:02d}:{mins:02d}:{secs:02d}"
            
        data.append({
            'id': uid,
            'name': info.get('name', f"Instance {uid}"),
            'status': info['status'],
            'owner': info.get('owner', 'system'),
            'owner_role': owner_role,
            'uptime': uptime_str,
            'logs': info['logs'][-50:] # Send last 50 on load
        })
    return jsonify({'success': True, 'instances': data})

@app.route('/instances_view')
@login_required
@admin_required
def instances_view():
    return render_template('instances.html', user=session['username'], role=session['role'])

@app.route('/api/instances/download', methods=['POST'])
@login_required
@admin_required
def download_instances():
    data = request.json or {}
    instance_ids = data.get('instance_ids', [])
    
    if not instance_ids:
        return jsonify({'success': False, 'error': 'No instances selected'}), 400
        
    memory_file = io.BytesIO()
    
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for uid in instance_ids:
            if uid in instances:
                inst_path = instances[uid]['path']
                inst_name = instances[uid].get('name', uid)
                
                # Walk the directory
                for root, dirs, files in os.walk(inst_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # Create relative path inside the zip.
                        # We put each instance inside a folder named like "InstanceName_ID"
                        zip_folder_name = secure_filename(f"{inst_name}_{uid}")
                        rel_path = os.path.relpath(file_path, inst_path)
                        arcname = os.path.join(zip_folder_name, rel_path)
                        try:
                            zf.write(file_path, arcname)
                        except Exception as e:
                            logger.error(f"Failed to zip file {file_path}: {e}")

    memory_file.seek(0)
    
    # Send the zip buffer directly to client
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name='nexus_instances_export.zip'
    )

@app.route('/api/instances/bulk_upload', methods=['POST'])
@login_required
@admin_required
def bulk_upload():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file part provided'}), 400
    file = request.files['file']
    if file.filename == '' or not file.filename.endswith('.zip'):
        return jsonify({'success': False, 'error': 'Invalid file'}), 400

    temp_id = str(uuid.uuid4())[:8]
    temp_zip_path = os.path.join(UPLOAD_DIR, f'bulk_{temp_id}.zip')
    temp_extract_path = os.path.join(UPLOAD_DIR, f'bulk_extract_{temp_id}')
    
    file.save(temp_zip_path)
    
    try:
        os.makedirs(temp_extract_path, exist_ok=True)
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)
            
        # Iterate over top-level items
        restored_count = 0
        for item in os.listdir(temp_extract_path):
            item_path = os.path.join(temp_extract_path, item)
            if os.path.isdir(item_path):
                # item is expected to be "Name_OldID"
                parts = item.rsplit('_', 1)
                inst_name = parts[0] if len(parts) > 1 else item
                
                new_uid = str(uuid.uuid4())[:8]
                new_inst_path = os.path.join(INSTANCES_DIR, new_uid)
                
                # Move the directory
                shutil.move(item_path, new_inst_path)
                
                # Register the instance
                instances[new_uid] = {
                    'process': None,
                    'status': 'stopped',
                    'logs': [],
                    'path': new_inst_path,
                    'name': inst_name,
                    'owner': session['username']
                }
                save_instances_persist()
                restored_count += 1
                
        return jsonify({'success': True, 'message': f'Successfully restored {restored_count} instances.'})
    except Exception as e:
        logger.error(f"Bulk extraction failed: {e}")
        return jsonify({'success': False, 'error': f'Failed to process zip: {str(e)}'}), 500
    finally:
        # Cleanup
        if os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path, ignore_errors=True)

@app.route('/api/instances/bulk_start', methods=['POST'])
@login_required
@admin_required
def bulk_start():
    data = request.json or {}
    instance_ids = data.get('instance_ids', [])
    for uid in instance_ids:
        if uid in instances:
            info = instances[uid]
            if info['status'] not in ['running', 'installing']:
                app_py_path = os.path.join(info['path'], 'app.py')
                if os.path.exists(app_py_path):
                    info['status'] = 'installing'
                    socketio.emit('status_update', {'id': uid, 'status': 'installing'}, namespace='/')
                    # Launch startup sequence in a background thread
                    threading.Thread(target=startup_sequence, args=(uid,), daemon=True).start()
                else:
                    logger.error(f"app.py not found for {uid} during bulk start")
    return jsonify({'success': True})

@app.route('/api/instances/bulk_stop', methods=['POST'])
@login_required
@admin_required
def bulk_stop():
    data = request.json or {}
    instance_ids = data.get('instance_ids', [])
    for uid in instance_ids:
        if uid in instances and instances[uid]['process'] is not None:
            try:
                instances[uid]['process'].terminate()
                instances[uid]['status'] = 'stopped'
                socketio.emit('status_update', {'id': uid, 'status': 'stopped'}, namespace='/')
            except Exception as e:
                logger.error(f"Bulk stop error on {uid}: {e}")
    return jsonify({'success': True})

@app.route('/api/instances/bulk_delete', methods=['POST'])
@login_required
@admin_required
def bulk_delete():
    data = request.json or {}
    instance_ids = data.get('instance_ids', [])
    for uid in instance_ids:
        if uid in instances:
            # Terminate if running
            if instances[uid]['process'] is not None:
                try:
                    instances[uid]['process'].terminate()
                except:
                    pass
            # Delete directory
            path = instances[uid]['path']
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=True)
            # Remove from dict
            del instances[uid]
            save_instances_persist()
            socketio.emit('instance_deleted', {'id': uid}, namespace='/')
    return jsonify({'success': True})

@app.route('/api/admin/settings', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_settings():
    accounts = load_accounts()
    if '__settings__' not in accounts:
        accounts['__settings__'] = {'default_max_instances': 1}
        
    if request.method == 'POST':
        data = request.json or {}
        new_limit = data.get('default_max_instances')
        cpu_limit = data.get('global_cpu_limit')
        
        updated = False
        if new_limit is not None:
            try:
                accounts['__settings__']['default_max_instances'] = int(new_limit)
                updated = True
            except ValueError:
                pass
                
        if cpu_limit is not None:
            try:
                accounts['__settings__']['global_cpu_limit'] = int(cpu_limit)
                updated = True
            except ValueError:
                pass
                
        if updated:
            save_accounts(accounts)
            return jsonify({'success': True, 'message': 'Global settings updated.'})
            
        return jsonify({'success': False, 'error': 'Invalid data provided.'}), 400
        
    return jsonify({'success': True, 'settings': accounts.get('__settings__', {})})

@app.route('/api/admin/users', methods=['GET'])
@login_required
@admin_required
def get_users():
    accounts = load_accounts()
    user_list = []
    
    settings = accounts.get('__settings__', {})
    default_limit = settings.get('default_max_instances', 1)
    
    for uname, info in accounts.items():
        if uname == '__settings__':
            continue
        base_limit = 999 if info.get('role') == 'admin' else default_limit
        user_list.append({
            'username': uname,
            'role': info.get('role', 'user'),
            'max_instances': info.get('max_instances', base_limit),
            'is_paused': info.get('is_paused', False)
        })
    return jsonify({'success': True, 'users': user_list})

@app.route('/api/admin/users/<username>/pause', methods=['POST'])
@login_required
@admin_required
def pause_user(username):
    accounts = load_accounts()
    if username not in accounts or accounts[username].get('role') == 'admin':
        return jsonify({'success': False, 'error': 'User not found or cannot pause admin'}), 400
        
    accounts[username]['is_paused'] = True
    save_accounts(accounts)
    
    # Auto-stop all their running instances
    for uid, info in instances.items():
        if info.get('owner') == username and info['status'] == 'running':
            if info['process'] is not None:
                try:
                    info['process'].terminate()
                    info['status'] = 'stopped'
                    socketio.emit('status_update', {'id': uid, 'status': 'stopped'}, namespace='/')
                except Exception as e:
                    logger.error(f"Failed to auto-stop {uid} during pause: {e}")
                    
    return jsonify({'success': True})

@app.route('/api/admin/users/<username>/resume', methods=['POST'])
@login_required
@admin_required
def resume_user(username):
    accounts = load_accounts()
    if username not in accounts:
        return jsonify({'success': False, 'error': 'User not found'}), 404
        
    accounts[username]['is_paused'] = False
    save_accounts(accounts)
    return jsonify({'success': True})

@app.route('/api/admin/users/<username>/delete', methods=['POST'])
@login_required
@admin_required
def delete_user(username):
    accounts = load_accounts()
    if username not in accounts or accounts[username].get('role') == 'admin':
        return jsonify({'success': False, 'error': 'User not found or cannot delete admin'}), 400
        
    del accounts[username]
    save_accounts(accounts)
    
    # Delete all their instances
    to_delete = [uid for uid, info in instances.items() if info.get('owner') == username]
    for uid in to_delete:
        if instances[uid]['process'] is not None:
            try:
                instances[uid]['process'].terminate()
            except:
                pass
        path = instances[uid]['path']
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        del instances[uid]
        socketio.emit('instance_deleted', {'id': uid}, namespace='/')
        
    save_instances_persist()
    return jsonify({'success': True})

@app.route('/api/admin/users/<username>/limit', methods=['POST'])
@login_required
@admin_required
def update_user_limit(username):
    data = request.json or {}
    new_limit = data.get('limit')
    
    if new_limit is None:
        return jsonify({'success': False, 'error': 'Limit required'}), 400
        
    try:
        new_limit = int(new_limit)
    except ValueError:
        return jsonify({'success': False, 'error': 'Limit must be an integer'}), 400
        
    accounts = load_accounts()
    if username not in accounts:
        return jsonify({'success': False, 'error': 'User not found'}), 404
        
    accounts[username]['max_instances'] = new_limit
    save_accounts(accounts)
    return jsonify({'success': True, 'message': f'Limit updated to {new_limit}'})

@app.route('/api/admin/users/<username>/role', methods=['POST'])
@login_required
@admin_required
def update_user_role(username):
    data = request.json or {}
    new_role = data.get('role')
    
    if new_role not in ['admin', 'user', 'paid user']:
        return jsonify({'success': False, 'error': 'Invalid role'}), 400
        
    accounts = load_accounts()
    if username not in accounts:
        return jsonify({'success': False, 'error': 'User not found'}), 404
        
    accounts[username]['role'] = new_role
    save_accounts(accounts)
    return jsonify({'success': True, 'message': f'Role for {username} updated to {new_role}'})

@app.route('/api/admin/full_backup', methods=['GET'])
@login_required
@admin_required
def full_backup():
    try:
        save_instances_persist()
        accounts = load_accounts()
        
        backup_data = {
            'timestamp': time.time(),
            'total_users': len([u for u in accounts if u != '__settings__']),
            'total_instances': len(instances),
            'users': {}
        }
        
        for uname, info in accounts.items():
            if uname == '__settings__':
                continue
                
            user_instances = []
            for uid, inst in instances.items():
                if inst.get('owner') == uname:
                    user_instances.append({
                        'id': uid,
                        'name': inst.get('name', 'Unnamed Instance'),
                        'status': inst.get('status', 'unknown'),
                        'path': inst.get('path', '')
                    })
                    
            backup_data['users'][uname] = {
                'role': info.get('role', 'user'),
                'max_instances': info.get('max_instances', 1),
                'is_paused': info.get('is_paused', False),
                'instances_count': len(user_instances),
                'instances': user_instances
            }
            
        temp_path = os.path.join(tempfile.gettempdir(), f'nexus_backup_{int(time.time())}.zip')
        ignored_dirs = {'__pycache__', '.git', 'venv', '.venv', 'node_modules', '.antigravity', '.gemini'}
        
        with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('server_snapshot.json', json.dumps(backup_data, indent=4).encode('utf-8'))
            for root, dirs, files in os.walk(BASE_DIR):
                dirs[:] = [d for d in dirs if d not in ignored_dirs]
                for file in files:
                    file_path = os.path.join(root, file)
                    if file_path == temp_path:
                        continue
                    rel_path = os.path.relpath(file_path, BASE_DIR)
                    try:
                        zf.write(file_path, rel_path)
                    except Exception as e:
                        logger.warning(f"Failed to zip {file_path}: {e}")
                        
        def delayed_delete(path):
            time.sleep(300)
            try:
                if os.path.exists(path):
                    os.remove(path)
            except:
                pass
                
        threading.Thread(target=delayed_delete, args=(temp_path,), daemon=True).start()
        
        return send_file(
            temp_path,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'full_server_backup_{int(time.time())}.zip'
        )
    except Exception as e:
        logger.error(f"Full backup error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admin/restore_backup', methods=['POST'])
@login_required
@admin_required
def restore_backup():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file part provided'}), 400
    file = request.files['file']
    if file.filename == '' or not file.filename.endswith('.zip'):
        return jsonify({'success': False, 'error': 'Invalid file'}), 400

    temp_zip_path = os.path.join(tempfile.gettempdir(), f'restore_{int(time.time())}.zip')
    file.save(temp_zip_path)
    
    try:
        # Stop all running instances first
        for uid, info in list(instances.items()):
            if info['process'] and info['process'].poll() is None:
                try:
                    info['process'].terminate()
                    info['process'].wait(timeout=2)
                except:
                    info['process'].kill()
            if os.path.exists(info['path']):
                shutil.rmtree(info['path'], ignore_errors=True)
                
        instances.clear()
        
        # Extract files cleanly
        with zipfile.ZipFile(temp_zip_path, 'r') as zf:
            for member in zf.namelist():
                if member in ['accounts.json', 'server_snapshot.json', 'instances.json'] or member.startswith('instances/'):
                    try:
                        zf.extract(member, BASE_DIR)
                    except Exception as e:
                        logger.warning(f"Error extracting {member}: {e}")
                        
        global accounts
        accounts = load_accounts()
        
        snapshot_path = os.path.join(BASE_DIR, 'server_snapshot.json')
        if os.path.exists(snapshot_path):
            with open(snapshot_path, 'r') as f:
                snapshot = json.load(f)
                users_data = snapshot.get('users', {})
                for uname, uinfo in users_data.items():
                    for inst in uinfo.get('instances', []):
                        uid = inst['id']
                        instances[uid] = {
                            'process': None,
                            'status': 'stopped',
                            'logs': [],
                            'path': os.path.join(INSTANCES_DIR, uid),
                            'name': inst.get('name', f'Instance {uid}'),
                            'owner': uname
                        }
                save_instances_persist()
        
        def restart_server():
            time.sleep(2)
            os.execl(sys.executable, sys.executable, *sys.argv)
            
        threading.Thread(target=restart_server, daemon=True).start()
        
        return jsonify({'success': True, 'message': 'Backup restored successfully'})
    except Exception as e:
        logger.error(f"Restore backup error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/users_view')
@login_required
@admin_required
def users_view():
    return render_template('users.html', user=session['username'], role=session['role'])

@app.route('/api/instances/create', methods=['POST'])
@login_required
def create_instance():
    data = request.json or {}
    guild_id = data.get('guild_id', '').strip()
    if not guild_id:
        return jsonify({'success': False, 'error': 'Guild ID is required.'}), 400
        
    try:
        count = int(data.get('count', 1))
    except (TypeError, ValueError):
        count = 1
        
    if count < 1: count = 1
    if count > 50: count = 50
    
    # Enforce limit based on DB
    if session['role'] != 'admin':
        accounts = load_accounts()
        user_info = accounts.get(session['username'], {})
        # Fallback to 1
        max_allowed = user_info.get('max_instances', 1) 
        
        owned_count = sum(1 for i in instances.values() if i.get('owner') == session['username'])
        if owned_count + count > max_allowed:
            return jsonify({'success': False, 'error': f'Cannot create {count} instances. Maximum instance limit ({max_allowed}) would be exceeded (you currently have {owned_count}).'}), 403

    created_ids = []
    failed = 0
    
    for i in range(count):
        instance_id = str(uuid.uuid4())[:8]
        instance_path = os.path.join(INSTANCES_DIR, instance_id)
        inst_name = guild_id
        
        try:
            os.makedirs(instance_path, exist_ok=True)
            status_to_set = 'empty'
            auto_start = False
            
            default_zip_path = os.path.join(BASE_DIR, 'default.zip')
            if os.path.exists(default_zip_path):
                try:
                    with zipfile.ZipFile(default_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(instance_path)
                    
                    # Provision guild.json
                    guild_json_path = os.path.join(instance_path, 'guild.json')
                    with open(guild_json_path, 'w') as f:
                        json.dump({"guild_id": guild_id}, f, indent=4)
                    
                    status_to_set = 'installing'
                    auto_start = True
                except Exception as e:
                    logger.error(f"Failed to auto-provision {instance_id} with default zip: {e}")
                    status_to_set = 'stopped'
                        
            instances[instance_id] = {
                'process': None,
                'status': status_to_set,
                'logs': [],
                'path': instance_path,
                'name': inst_name,
                'owner': session['username']
            }
            save_instances_persist()
            created_ids.append(instance_id)

            if auto_start:
                threading.Thread(target=startup_sequence, args=(instance_id,), daemon=True).start()

        except Exception as e:
            logger.error(f"Creation failed for one instance: {e}")
            failed += 1
            
    if failed > 0 and len(created_ids) == 0:
        return jsonify({'success': False, 'error': 'Failed to create instances. Check server logs.'}), 500
        
    return jsonify({'success': True, 'ids': created_ids})

@app.route('/api/instances/<instance_id>/upload', methods=['POST'])
@login_required
def upload_file(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    if session['role'] != 'admin':
        return jsonify({'success': False, 'error': 'Only admins can upload custom console zip files.'}), 403
        
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file part provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No selected file'}), 400
        
    if file and file.filename.endswith('.zip'):
        instance_path = instances[instance_id]['path']
        
        # Save temporary zip
        zip_path = os.path.join(instance_path, 'temp.zip')
        file.save(zip_path)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(instance_path)
            os.remove(zip_path) # cleanup
            
            instances[instance_id]['status'] = 'stopped'
            socketio.emit('status_update', {'id': instance_id, 'status': 'stopped'}, namespace='/')
            return jsonify({'success': True, 'message': 'Package extracted successfully.'})
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return jsonify({'success': False, 'error': f'Failed to extract zip: {str(e)}'}), 500
            
    return jsonify({'success': False, 'error': 'Invalid file type. Only .zip allowed.'}), 400

@app.route('/api/instances/<instance_id>/start', methods=['POST'])
@login_required
def start_instance(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    owner = info.get('owner')
    accounts = load_accounts()
    
    if session['role'] != 'admin':
        if owner != session['username']:
            return jsonify({'success': False, 'error': 'Permission denied'}), 403
        if accounts.get(session['username'], {}).get('is_paused', False):
            return jsonify({'success': False, 'error': 'Your account is paused. Please contact an administrator.'}), 403
    
    if info['status'] in ['running', 'installing']:
        return jsonify({'success': False, 'error': 'Instance is already running or installing'}), 400
        
    app_py_path = os.path.join(info['path'], 'app.py')
    if not os.path.exists(app_py_path):
        return jsonify({'success': False, 'error': 'app.py not found in package.'}), 400

    instances[instance_id]['status'] = 'installing'
    socketio.emit('status_update', {'id': instance_id, 'status': 'installing'}, namespace='/')
    
    # Launch startup sequence in a background thread to prevent blocking the API request
    threading.Thread(target=startup_sequence, args=(instance_id,), daemon=True).start()
    
    return jsonify({'success': True, 'message': 'Starting instance...'})

def startup_sequence(instance_id):
    info = instances.get(instance_id)
    if not info: return
    
    instance_path = info['path']
    try:
        req_path = os.path.join(instance_path, 'requirements.txt')
        if os.path.exists(req_path):
            socketio.emit('log_update', {'id': instance_id, 'log': 'Found requirements.txt. Installing dependencies...'}, namespace='/')
            try:
                subprocess.run(
                    ['pip', 'install', '-r', 'requirements.txt'],
                    cwd=instance_path,
                    check=True,
                    capture_output=True,
                    text=True
                )
                socketio.emit('log_update', {'id': instance_id, 'log': 'Dependencies installed successfully.'}, namespace='/')
            except subprocess.CalledProcessError as e:
                socketio.emit('log_update', {'id': instance_id, 'log': f'Warning: Failed to install some dependencies. \n{e.stderr}'}, namespace='/')

        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'

        process = subprocess.Popen(
            ['python', 'app.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=instance_path,
            env=env,
            bufsize=1
        )

        info['process'] = process
        info['status'] = 'running'
        info['start_time'] = time.time()
        
        socketio.emit('status_update', {'id': instance_id, 'status': 'running'}, namespace='/')
        
        thread = threading.Thread(target=stream_logs, args=(instance_id, process))
        thread.daemon = True
        thread.start()
        
    except Exception as e:
        logger.error(f"Startup sequence failed for {instance_id}: {e}")
        info['status'] = 'stopped'
        socketio.emit('status_update', {'id': instance_id, 'status': 'stopped'}, namespace='/')
        socketio.emit('log_update', {'id': instance_id, 'log': f'Error starting instance: {e}'}, namespace='/')


@app.route('/api/instances/<instance_id>/stop', methods=['POST'])
@login_required
def stop_instance(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    if session['role'] != 'admin' and info.get('owner') != session['username']:
        return jsonify({'success': False, 'error': 'Permission denied'}), 403

    if info['process'] and info['process'].poll() is None:
        info['process'].terminate()
        try:
            info['process'].wait(timeout=3)
        except subprocess.TimeoutExpired:
            info['process'].kill() # Force kill if stubborn
            
        info['status'] = 'stopped'
        socketio.emit('status_update', {'id': instance_id, 'status': 'stopped'}, namespace='/')
        return jsonify({'success': True, 'message': 'Instance stopped'})
        
    return jsonify({'success': False, 'error': 'Instance already stopped or invalid'}), 400

@app.route('/api/instances/<instance_id>', methods=['DELETE'])
@login_required
def delete_instance(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    if session['role'] != 'admin' and info.get('owner') != session['username']:
        return jsonify({'success': False, 'error': 'Permission denied'}), 403
    
    # Ensure it's stopped
    if info['process'] and info['process'].poll() is None:
        info['process'].kill()
        
    # Attempt to wait for file locks to release
    time.sleep(0.5)
    
    try:
        shutil.rmtree(info['path'], ignore_errors=True)
    except Exception as e:
        logger.warning(f"Could not fully delete {info['path']}: {e}")
        
    del instances[instance_id]
    save_instances_persist()
    socketio.emit('instance_deleted', {'id': instance_id}, namespace='/')
    return jsonify({'success': True, 'message': 'Instance deleted'})

@app.route('/api/instances/<instance_id>/restart', methods=['POST'])
@login_required
def restart_instance(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    owner = info.get('owner')
    accounts = load_accounts()
    
    if session['role'] != 'admin':
        if owner != session['username']:
            return jsonify({'success': False, 'error': 'Permission denied'}), 403
        if accounts.get(session['username'], {}).get('is_paused', False):
            return jsonify({'success': False, 'error': 'Your account is paused. Please contact an administrator.'}), 403
    
    # 1. Stop existing process
    if info['process'] and info['process'].poll() is None:
        info['process'].terminate()
        try:
            info['process'].wait(timeout=3)
        except subprocess.TimeoutExpired:
            info['process'].kill()
            
    info['status'] = 'stopped'
    socketio.emit('status_update', {'id': instance_id, 'status': 'restarting'}, namespace='/')
    time.sleep(0.5)
    
    # 2. Start new process
    threading.Thread(target=startup_sequence, args=(instance_id,), daemon=True).start()
    return jsonify({'success': True, 'message': 'Restarted Successfully'})

@app.route('/api/instances/<instance_id>/credentials', methods=['GET', 'POST'])
@login_required
def manage_credentials(instance_id):
    if instance_id not in instances:
        return jsonify({'success': False, 'error': 'Instance not found'}), 404
        
    info = instances[instance_id]
    if session['role'] != 'admin' and info.get('owner') != session['username']:
        return jsonify({'success': False, 'error': 'Permission denied'}), 403
        
    saved_json_path = os.path.join(info['path'], 'saved.json')
    
    if request.method == 'GET':
        if not os.path.exists(saved_json_path):
            return jsonify({'success': True, 'uid': '', 'password': ''})
            
        try:
            with open(saved_json_path, 'r') as f:
                data = json.load(f)
                if data:
                    uid = list(data.keys())[0]
                    password = data[uid]
                    return jsonify({'success': True, 'uid': uid, 'password': password})
        except Exception as e:
            logger.error(f"Error reading credentials for {instance_id}: {e}")
            
        return jsonify({'success': True, 'uid': '', 'password': ''})
        
    if request.method == 'POST':
        data = request.json or {}
        new_uid = data.get('uid', '').strip()
        new_pass = data.get('password', '').strip()
        
        if not new_uid or not new_pass:
            return jsonify({'success': False, 'error': 'UID and Password are required'}), 400
            
        try:
            with open(saved_json_path, 'w') as f:
                json.dump({new_uid: new_pass}, f, indent=4)
            return jsonify({'success': True, 'message': 'Credentials updated successfully'})
        except Exception as e:
            logger.error(f"Error saving credentials for {instance_id}: {e}")
            return jsonify({'success': False, 'error': 'Failed to save credentials'}), 500

@socketio.on('connect')
def handle_connect():
    logger.info("Client connected to SocketIO")

@app.errorhandler(404)
def not_found(e):
    # Ensure that undefined API routes return JSON, while UI routes can return templates
    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'error': 'API endpoint not found'}), 404
    return "Page Not Found", 404

def discord_auto_backup_thread():
    """Background thread to backup the server to Discord every 30 minutes."""
    webhook_url = "https://discord.com/api/webhooks/1482718948603461733/SJvgbltB6jVR5fwx_WN-h8H0FVZp5-siMp0xBeKRJkMze2_6IbBMn0lqVLvYfMwR3Wck"
    logger.info("Starting background Discord auto-backup thread (every 30 mins)...")
    
    # Wait for the server to fully start up before doing the first backup
    time.sleep(30)
    
    while True:
        try:
            logger.info("Starting scheduled Discord auto-backup...")
            save_instances_persist()
            accounts = load_accounts()
            
            backup_data = {
                'timestamp': time.time(),
                'total_users': len([u for u in accounts if u != '__settings__']),
                'total_instances': len(instances),
                'users': {}
            }
            
            for uname, info in accounts.items():
                if uname == '__settings__':
                    continue
                    
                user_instances = []
                for uid, inst in instances.items():
                    if inst.get('owner') == uname:
                        user_instances.append({
                            'id': uid,
                            'name': inst.get('name', 'Unnamed Instance'),
                            'status': inst.get('status', 'unknown'),
                            'path': inst.get('path', '')
                        })
                        
                backup_data['users'][uname] = {
                    'role': info.get('role', 'user'),
                    'max_instances': info.get('max_instances', 1),
                    'is_paused': info.get('is_paused', False),
                    'instances_count': len(user_instances),
                    'instances': user_instances
                }
                
            temp_path = os.path.join(tempfile.gettempdir(), f'nexus_discord_backup_{int(time.time())}.zip')
            ignored_dirs = {'__pycache__', '.git', 'venv', '.venv', 'node_modules', '.antigravity', '.gemini'}
            
            with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr('server_snapshot.json', json.dumps(backup_data, indent=4).encode('utf-8'))
                for root, dirs, files in os.walk(BASE_DIR):
                    dirs[:] = [d for d in dirs if d not in ignored_dirs]
                    for file in files:
                        file_path = os.path.join(root, file)
                        if file_path == temp_path:
                            continue
                        rel_path = os.path.relpath(file_path, BASE_DIR)
                        try:
                            zf.write(file_path, rel_path)
                        except Exception as e:
                            logger.warning(f"Failed to zip {file_path}: {e}")
                            
            file_name = f"auto_backup_{int(time.time())}.zip"
            with open(temp_path, 'rb') as f:
                response = requests.post(webhook_url, files={'file': (file_name, f)})
            
            if response.status_code in [200, 204]:
                logger.info("Successfully uploaded auto-backup to Discord.")
            else:
                logger.error(f"Failed to upload backup to Discord: {response.status_code} - {response.text}")
                
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error in Discord auto-backup thread: {e}")
            
        time.sleep(1800)  # 30 minutes = 1800 seconds

def cpu_monitor_thread():
    """Background thread to monitor global CPU usage and enforce limits."""
    logger.info("Starting background CPU monitor thread...")
    while True:
        try:
            time.sleep(5) # Check every 5 seconds
            accounts = load_accounts()
            settings = accounts.get('__settings__', {})
            # Default to 60 if not set, or 100 to effectively disable if desired (currently 60 requested)
            cpu_limit = settings.get('global_cpu_limit', 60)
            
            # Get current global CPU usage
            current_cpu = psutil.cpu_percent(interval=1)
            
            if current_cpu > cpu_limit:
                # Limit breached, stop all non-admin instances
                logger.warning(f"Global CPU ({current_cpu}%) exceeded limit ({cpu_limit}%). Stopping user instances.")
                
                for uid, info in list(instances.items()):
                    if info['status'] == 'running':
                        owner = info.get('owner', '')
                        # Check owner role
                        owner_role = accounts.get(owner, {}).get('role', 'user')
                        
                        if owner_role != 'admin':
                            logger.info(f"Auto-stopping instance {uid} (Owned by {owner}) due to CPU limits.")
                            if info['process'] and info['process'].poll() is None:
                                try:
                                    info['process'].terminate()
                                    info['process'].wait(timeout=2)
                                except subprocess.TimeoutExpired:
                                    info['process'].kill()
                                except:
                                    pass
                                    
                            info['status'] = 'stopped'
                            socketio.emit('status_update', {'id': uid, 'status': 'stopped'}, namespace='/')
                            socketio.emit('log_update', {'id': uid, 'log': f'SYSTEM: Instance auto-stopped. Global server CPU limit ({cpu_limit}%) exceeded.'}, namespace='/')
                            
        except Exception as e:
            logger.error(f"Error in CPU monitor thread: {e}")
            time.sleep(5)

if __name__ == '__main__':
    # Start monitor thread
    monitor = threading.Thread(target=cpu_monitor_thread, daemon=True)
    monitor.start()
    discord_backup = threading.Thread(target=discord_auto_backup_thread, daemon=True)
    discord_backup.start()

    # Running on 0.0.0.0 port 5000 for global access
    logger.info("Starting Ultra Pro Console Manager on http://0.0.0.0:5000")
    socketio.run(app, host='0.0.0.0', port=10000, debug=False)
