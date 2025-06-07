import tkinter as tk
from tkinter import ttk
import os
import hashlib
import sqlite3
from datetime import datetime
import threading
import mimetypes
import json
from concurrent.futures import ThreadPoolExecutor
import subprocess

# Caminho do banco de dados e do ffprobe
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'video_data.db')
ffprobe_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ffprobe.exe')

def init_database():
    conn = sqlite3.connect(db_path, check_same_thread=False)
    c = conn.cursor()
    # Verificar se a tabela já existe
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
    if not c.fetchone():
        c.execute('''
            CREATE TABLE files (
                file_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                extension TEXT,
                file_path TEXT NOT NULL,
                size_bytes REAL,
                modified_at TIMESTAMP,
                hash TEXT,
                duration_seconds REAL,
                resolution TEXT,
                fps REAL,
                video_codec TEXT,
                bitrate_total_kbps INTEGER
            )
        ''')
        c.execute('CREATE INDEX idx_file_path ON files(file_path)')
    conn.commit()
    conn.close()

def calculate_hash(file_path, max_bytes=2*1024*1024):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            bytes_read = 0
            while bytes_read < max_bytes:
                byte_block = f.read(4096)
                if not byte_block:
                    break
                sha256.update(byte_block)
                bytes_read += len(byte_block)
            file_size = os.stat(file_path).st_size
            if file_size > max_bytes * 2:
                f.seek(file_size // 2)
                bytes_read = 0
                while bytes_read < max_bytes:
                    byte_block = f.read(4096)
                    if not byte_block:
                        break
                    sha256.update(byte_block)
                    bytes_read += len(byte_block)
        return sha256.hexdigest()
    except Exception:
        return None

def get_video_metadata(file_path):
    try:
        if not os.path.exists(ffprobe_path):
            return {"error": "ffprobe.exe não encontrado no diretório do aplicativo"}
        
        normalized_path = os.path.normpath(file_path)
        if any(ord(char) > 127 for char in normalized_path) and os.name == 'nt':
            normalized_path = '\\?\\' + normalized_path.replace('/', '\\')
        
        cmd = [
            ffprobe_path,
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            normalized_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        metadata = json.loads(result.stdout)
        
        extracted = {}
        video_stream = next((stream for stream in metadata.get('streams', []) if stream['codec_type'] == 'video'), None)
        if video_stream:
            if 'width' in video_stream and 'height' in video_stream:
                extracted['resolution'] = f"{video_stream['width']}x{video_stream['height']}"
            if 'r_frame_rate' in video_stream:
                try:
                    num, denom = map(int, video_stream['r_frame_rate'].split('/'))
                    extracted['fps'] = num / denom if denom != 0 else 0
                except (ValueError, ZeroDivisionError):
                    pass
            if 'codec_name' in video_stream:
                extracted['video_codec'] = video_stream['codec_name']
        
        format_data = metadata.get('format', {})
        if 'duration' in format_data:
            try:
                extracted['duration_seconds'] = float(format_data['duration'])
            except ValueError:
                pass
        if 'bit_rate' in format_data:
            try:
                extracted['bitrate_total_kbps'] = int(format_data['bit_rate']) // 1000
            except ValueError:
                pass
        
        return extracted if extracted else {"error": "Nenhum metadado relevante encontrado"}
    except Exception as e:
        return {"error": f"Erro ao extrair metadados: {str(e)}"}

def is_video_file(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type and mime_type.startswith('video')

def save_to_db(data):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('SELECT 1 FROM files WHERE file_id = ?', (data['file_id'],))
        exists = c.fetchone() is not None
        
        update_fields = {
            'name': data['name'],
            'extension': data['extension'],
            'file_path': data['file_path'],
            'size_bytes': data['size_bytes'],
            'modified_at': data['modified_at']
        }
        
        if 'hash' in data:
            update_fields['hash'] = data['hash']
        if 'metadata' in data:
            update_fields.update({
                'duration_seconds': data['metadata'].get('duration_seconds'),
                'resolution': data['metadata'].get('resolution'),
                'fps': data['metadata'].get('fps'),
                'video_codec': data['metadata'].get('video_codec'),
                'bitrate_total_kbps': data['metadata'].get('bitrate_total_kbps')
            })
        
        fields = ', '.join([f'{k} = ?' for k in update_fields.keys()])
        values = list(update_fields.values()) + [data['file_id']]
        
        if exists:
            c.execute(f'UPDATE files SET {fields} WHERE file_id = ?', values)
        else:
            columns = ', '.join(['file_id'] + list(update_fields.keys()))
            placeholders = ', '.join(['?'] * (len(update_fields) + 1))
            c.execute(f'INSERT INTO files ({columns}) VALUES ({placeholders})', 
                     [data['file_id']] + list(update_fields.values()))
        
        conn.commit()
    finally:
        conn.close()

def process_file(file_path, step, messages, lock):
    if not is_video_file(file_path):
        with lock:
            messages.append(f"Ignorando {file_path} (não é vídeo)\n")
        return False
    
    stats = os.stat(file_path)
    file_id = hashlib.sha256(file_path.encode()).hexdigest()
    
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        c = conn.cursor()
        c.execute('SELECT hash, duration_seconds, resolution, video_codec FROM files WHERE file_path = ?', (file_path,))
        result = c.fetchone()
        
        if result:
            db_hash, duration, resolution, video_codec = result
            if step == 2 and db_hash:
                current_hash = calculate_hash(file_path)
                if current_hash == db_hash:
                    with lock:
                        messages.append(f"Pulando {file_path} (hash inalterado)\n")
                    return False
            if step == 1 and duration and resolution and video_codec:
                with lock:
                    messages.append(f"Pulando {file_path} (metadados completos)\n")
                return False
    finally:
        conn.close()
    
    data = {
        'file_id': file_id,
        'name': os.path.basename(file_path),
        'extension': os.path.splitext(file_path)[1],
        'file_path': file_path,
        'size_bytes': stats.st_size,
        'modified_at': datetime.fromtimestamp(stats.st_mtime).isoformat()
    }
    
    if step == 2:
        try:
            data['hash'] = calculate_hash(file_path)
            if not data['hash']:
                with lock:
                    messages.append(f"Erro ao calcular hash para {file_path}\n")
                return False
            with lock:
                messages.append(f"Hash calculado para {file_path}\n")
        except Exception as e:
            with lock:
                messages.append(f"Erro ao calcular hash para {file_path}: {e}\n")
            return False
    elif step == 1:
        try:
            metadata = get_video_metadata(file_path)
            if metadata.get('error'):
                with lock:
                    messages.append(f"Erro nos metadados de {file_path}: {metadata['error']}\n")
                return False
            data['metadata'] = metadata
            with lock:
                messages.append(f"Metadados coletados para {file_path}\n")
        except Exception as e:
            with lock:
                messages.append(f"Erro ao coletar metadados para {file_path}: {e}\n")
            return False
    
    try:
        save_to_db(data)
        return True
    except Exception as e:
        with lock:
            messages.append(f"Erro ao salvar dados no DB para {file_path}: {e}\n")
        return False

def update_log(messages, lock, text_widget, root):
    with lock:
        while messages:
            message = messages.pop(0)
            text_widget.insert(tk.END, message)
            text_widget.see(tk.END)
    root.update()
    root.after(100, update_log, messages, lock, text_widget, root)

def process_folder(folder_path, text_widget, messages, lock, btn, step=1):
    btn.config(state='disabled')
    text_widget.delete(1.0, tk.END)
    
    if not os.path.isdir(folder_path):
        with lock:
            messages.append(f"Caminho inválido: {folder_path}\n")
        btn.config(state='normal')
        return
    
    with lock:
        messages.append(f"Processando pasta: {folder_path} (Etapa {step})\n")
        messages.append("Coletando lista de arquivos...\n")
    
    video_files = []
    file_count = 0
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if is_video_file(file_path):
                video_files.append(file_path)
                file_count += 1
                if file_count % 10 == 0:
                    with lock:
                        messages.append(f"Encontrados {file_count} arquivos de vídeo...\n")
    
    if not video_files:
        with lock:
            messages.append("Nenhum arquivo de vídeo encontrado.\n")
        btn.config(state='normal')
        return
    
    with lock:
        messages.append(f"Encontrados {len(video_files)} arquivos de vídeo para processar.\n")
    
    processed = 0
    failed = 0
    skipped = 0
    
    with ThreadPoolExecutor(max_workers=8) as executor:  # Aumentado para 8 workers
        futures = [executor.submit(process_file, file_path, step, messages, lock) for file_path in video_files]
        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=30)
                if result:
                    processed += 1
                else:
                    skipped += 1
                with lock:
                    messages.append(f"Processando: {i+1}/{len(video_files)} (OK: {processed}, Erros: {failed}, Pulados: {skipped})\n")
            except TimeoutError:
                failed += 1
                with lock:
                    messages.append(f"Timeout ao processar arquivo {i+1}\n")
            except Exception as e:
                failed += 1
                with lock:
                    messages.append(f"Erro ao processar arquivo {i+1}: {e}\n")
    
    if step == 1 and (processed > 0 or skipped > 0):
        with lock:
            messages.append("Iniciando segunda etapa - coleta de hash\n")
        processed, failed, skipped = 0, 0, 0
 
        with ThreadPoolExecutor(max_workers=8) as executor:  # Aumentado para 8 workers
                futures = [executor.submit(process_file, file_path, 2, messages, lock) for file_path in video_files]
                for i, future in enumerate(futures):
                    try:
                        result = future.result(timeout=30)
                        if result:
                            processed += 1
                        else:
                            skipped += 1
                        with lock:
                            messages.append(f"Processando: {i+1}/{len(video_files)} (OK: {processed}, Erros: {failed}, Pulados: {skipped})\n")
                    except TimeoutError:
                        failed += 1
                        with lock:
                            messages.append(f"Timeout ao processar arquivo {i+1}\n")
                    except Exception as e:
                        failed += 1
                        with lock:
                            messages.append(f"Erro ao processar arquivo {i+1}: {e}\n")
    
    with lock:
        messages.append(f"\nProcessados {processed} vídeos. Dados salvos em {db_path}\n")
    btn.config(state='normal')

def start_process(entry_path, text_widget, btn):
    folder_path = entry_path.get().strip()
    if folder_path:
        messages = []
        lock = threading.Lock()
        threading.Thread(target=process_folder, args=(folder_path, text_widget, messages, lock, btn, 1), daemon=True).start()
        update_log(messages, lock, text_widget, root)

# Interface Tkinter
root = tk.Tk()
root.title("Hash e Metadados de Vídeos")
root.geometry("600x400")
frame = ttk.Frame(root, padding="10")
frame.grid(row=0, column=0, sticky="wens")

ttk.Label(frame, text="Caminho da Pasta:").grid(row=0, column=0, sticky=tk.W, pady=5)
entry_path = ttk.Entry(frame, width=50)
entry_path.grid(row=0, column=1, sticky=tk.W, pady=5)

btn_process = ttk.Button(frame, text="Processar Vídeos", command=lambda: start_process(entry_path, text_log, btn_process))
btn_process.grid(row=1, column=0, sticky=tk.W, pady=5)

text_log = tk.Text(frame, height=20, width=70)
text_log.grid(row=2, column=0, columnspan=2, pady=5)
scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=text_log.yview)
scrollbar.grid(row=2, column=2, sticky="ns")
text_log['yscrollcommand'] = scrollbar.set

init_database()
root.mainloop()