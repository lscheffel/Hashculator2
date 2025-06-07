import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import tkinter.font as tkfont
import sqlite3
import os
import subprocess
import sys

# Caminho do banco de dados e main.py
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'video_data.db')
main_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main.py')

def open_file(filepath):
    try:
        filepath = os.path.normpath(filepath)
        if os.path.exists(filepath):
            if sys.platform.startswith("win"):
                os.startfile(filepath)
            elif sys.platform.startswith("darwin"):
                subprocess.call(["open", filepath])
            else:
                subprocess.call(["xdg-open", filepath])
        else:
            raise FileNotFoundError
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o arquivo:\n{filepath}\n\nErro: {e}")

def load_data_from_db():
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT file_id, name, extension, file_path, size_bytes, modified_at, hash, duration_seconds, resolution, fps, video_codec, bitrate_total_kbps FROM files')
        columns = [desc[0] for desc in c.description]
        data = [dict(zip(columns, row)) for row in c.fetchall()]
        conn.close()
        
        for item in data:
            item['size_mb'] = round(item['size_bytes'] / (1024 ** 2), 2) if item['size_bytes'] else 0
            item['modified_at'] = item['modified_at'][:19] if item['modified_at'] else ''
            item['duration_seconds'] = round(item['duration_seconds'], 2) if item['duration_seconds'] else 0
            item['fps'] = round(item['fps'], 2) if item['fps'] else 0
            item['bitrate_total_kbps'] = item['bitrate_total_kbps'] if item['bitrate_total_kbps'] else 0
            item['hash'] = item['hash'] or ''  # Evita None
            item['resolution'] = item['resolution'] or ''
            item['video_codec'] = item['video_codec'] or ''
        return data
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao carregar dados do banco:\n{e}")
        return []

def get_common_stats(data):
    size_list = [item['size_bytes'] for item in data if item['size_bytes']]
    durations = [item['duration_seconds'] for item in data if item['duration_seconds']]
    
    total_seconds = sum(durations)
    days = int(total_seconds // (24 * 3600))
    hours = int((total_seconds % (24 * 3600)) // 3600)
    minutes = int((total_seconds % 3600) // 60)
    total_time = f"{days} dias, {hours} horas, {minutes} minutos"
    
    return {
        'Total de arquivos': len(data),
        'Tamanho total (MB)': round(sum(size_list) / (1024**2), 2) if size_list else 0,
        'Tamanho médio (MB)': round(sum(size_list) / len(size_list) / (1024**2), 2) if size_list else 0,
        'Duração média (s)': round(sum(durations) / len(durations), 2) if durations else 0,
        'Tempo total': total_time
    }

def create_filter_row(frame, display_columns, filters):
    entry_row = tk.Frame(frame)
    entry_row.pack(fill="x")

    for i, col in enumerate(display_columns):
        entry = ttk.Entry(entry_row)
        entry.grid(row=0, column=i, sticky="ew", padx=2)
        entry.insert(0, col)
        entry.config(foreground='grey')
        filters[col] = entry
        entry_row.grid_columnconfigure(i, weight=1)

        def on_focus_in(event, e=entry, placeholder=col):
            if e.get() == placeholder:
                e.delete(0, tk.END)
                e.config(foreground='black')

        def on_focus_out(event, e=entry, placeholder=col):
            if not e.get():
                e.insert(0, placeholder)
                e.config(foreground='grey')

        entry.bind("<FocusIn>", on_focus_in)
        entry.bind("<FocusOut>", on_focus_out)

def get_filtered_data(tree, original_data, columns, display_columns, filters):
    column_map = {dc: c for dc, c in zip(display_columns, columns)}
    filtered_data = []
    for item in original_data:
        match = True
        for display_col in display_columns:
            col = column_map[display_col]
            val = str(item.get(col, '')).lower()
            search = filters[display_col].get().lower()
            if search and search != display_col.lower() and search not in val:
                match = False
                break
        if match:
            filtered_data.append(item)
    return filtered_data

def apply_filters(tree, original_data, columns, display_columns, filters, filtered_stats_label):
    tree.delete(*tree.get_children())
    filtered_data = get_filtered_data(tree, original_data, columns, display_columns, filters)
    
    filtered_stats = get_common_stats(filtered_data)
    filtered_stats_text = "\n".join(f"{k}: {v}" for k, v in filtered_stats.items())
    filtered_stats_label.config(text=filtered_stats_text)
    
    for item in filtered_data:
        tree.insert("", "end", values=[item.get(col, '') for col in columns])

def export_playlist(tree, columns):
    selected_items = tree.selection()
    if not selected_items:
        messagebox.showwarning("Seleção vazia", "Selecione pelo menos um arquivo na tabela.")
        return
    
    paths = []
    for item_id in selected_items:
        values = tree.item(item_id)["values"]
        row_dict = dict(zip(columns, values))
        path = os.path.normpath(row_dict.get("file_path", ""))
        if os.path.exists(path):
            paths.append(path)
    
    if not paths:
        messagebox.showwarning("Exportação vazia", "Nenhum arquivo válido selecionado.")
        return
    
    filepath = filedialog.asksaveasfilename(defaultextension=".m3u", filetypes=[("M3U Playlist", "*.m3u")])
    if filepath:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for path in paths:
                f.write(f"{path}\n")
        messagebox.showinfo("Playlist salva", f"Playlist exportada para:\n{filepath}")

def sort_column(tree, col, reverse, columns):
    data = [(tree.set(k, col), k) for k in tree.get_children('')]
    col_index = columns.index(col)
    if col_index in [3, 4, 7, 8, 9]:  # size_mb, duration_seconds, fps, bitrate_total_kbps
        try:
            data.sort(key=lambda t: float(t[0] or 0), reverse=reverse)
        except ValueError:
            data.sort(key=lambda t: t[0].lower(), reverse=reverse)
    else:
        data.sort(key=lambda t: t[0].lower(), reverse=reverse)
    
    for index, (_, k) in enumerate(data):
        tree.move(k, '', index)
    tree.heading(col, command=lambda: sort_column(tree, col, not reverse, columns))

def adjust_column_widths(tree, columns, data):
    font = tkfont.nametofont("TkDefaultFont")
    max_widths = {}
    min_width = 50
    max_width = 300
    
    for col in columns:
        title_width = font.measure(col) // 8
        max_widths[col] = title_width
    
    for item in data:
        for col in columns:
            val = str(item.get(col, ''))
            width = font.measure(val) // 8
            max_widths[col] = max(max_widths[col], width)
    
    for col in columns:
        width = max(min_width, min(max_width, max_widths[col] + 10))
        tree.column(col, width=width, stretch=False)

def refresh_db(tree, columns, display_columns, filters, filtered_stats_label, stat_frame):
    data = load_data_from_db()
    if not data:
        return data
    
    stats = get_common_stats(data)
    for widget in stat_frame.winfo_children():
        widget.destroy()
    for k, v in stats.items():
        label = ttk.Label(stat_frame, text=f"{k}: {v}")
        label.pack(anchor="w")
    
    apply_filters(tree, data, columns, display_columns, filters, filtered_stats_label)
    adjust_column_widths(tree, display_columns, data)
    return data

def run_hashculator():
    try:
        if os.path.exists(main_script):
            subprocess.Popen([sys.executable, main_script], creationflags=subprocess.DETACHED_PROCESS if sys.platform.startswith("win") else 0)
        else:
            messagebox.showerror("Erro", f"Script {main_script} não encontrado.")
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao executar Hashculator:\n{e}")

def run_visualization():
    root = tk.Tk()
    root.title("Hashculator Viewer")
    root.geometry("1000x600")
    root.resizable(True, True)

    data = load_data_from_db()
    if not data:
        root.destroy()
        return

    stats = get_common_stats(data)

    style = ttk.Style()
    style.configure("Custom.Vertical.TScrollbar", width=16)
    style.configure("Custom.Horizontal.TScrollbar", height=16)

    stats_container = ttk.Frame(root)
    stats_container.pack(fill="x", padx=10, pady=5)

    stat_frame = ttk.LabelFrame(stats_container, text="Estatísticas Gerais")
    stat_frame.grid(row=0, column=0, sticky="n", padx=5)
    for k, v in stats.items():
        label = ttk.Label(stat_frame, text=f"{k}: {v}")
        label.pack(anchor="w")

    filtered_stat_frame = ttk.LabelFrame(stats_container, text="Estatísticas de Filtragem")
    filtered_stat_frame.grid(row=0, column=1, sticky="n", padx=5)
    filtered_stats_label = ttk.Label(filtered_stat_frame, text="Nenhum filtro aplicado")
    filtered_stats_label.pack(anchor="w")

    stats_container.grid_columnconfigure(0, weight=1)
    stats_container.grid_columnconfigure(1, weight=1)

    table_frame = ttk.Frame(root)
    table_frame.pack(fill="both", expand=True, padx=10, pady=5)

    columns = [
        'name', 'extension', 'file_path', 'size_mb', 'duration_seconds',
        'resolution', 'fps', 'video_codec', 'bitrate_total_kbps', 'modified_at', 'hash'
    ]
    display_columns = [
        'Nome', 'Extensão', 'Caminho', 'Tamanho (MB)', 'Duração (s)',
        'Resolução', 'FPS', 'Codec', 'Bitrate (kbps)', 'Modificado', 'Hash'
    ]

    filters = {}
    filter_frame = ttk.LabelFrame(table_frame, text="Filtros")
    filter_frame.pack(fill="x")
    create_filter_row(filter_frame, display_columns, filters)

    tree_frame = ttk.Frame(table_frame)
    tree_frame.pack(fill="both", expand=True)

    tree = ttk.Treeview(tree_frame, columns=display_columns, show="headings")
    for col, display_col in zip(columns, display_columns):
        tree.heading(display_col, text=display_col, command=lambda c=display_col: sort_column(tree, c, False, display_columns))
        tree.column(display_col, width=120, stretch=False)

    vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview, style="Custom.Vertical.TScrollbar")
    hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview, style="Custom.Horizontal.TScrollbar")
    tree.configure(yscroll=vsb.set, xscroll=hsb.set)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    def on_filter_change(event):
        apply_filters(tree, data, columns, display_columns, filters, filtered_stats_label)
        adjust_column_widths(tree, display_columns, get_filtered_data(tree, data, columns, display_columns, filters))

    for entry in filters.values():
        entry.bind("<KeyRelease>", on_filter_change)

    apply_filters(tree, data, columns, display_columns, filters, filtered_stats_label)
    adjust_column_widths(tree, display_columns, data)

    def on_double_click(event):
        item_id = tree.focus()
        if not item_id:
            return
        values = tree.item(item_id)["values"]
        row_dict = dict(zip(columns, values))
        path = row_dict.get("file_path")
        if path:
            open_file(path)

    tree.bind("<Double-1>", on_double_click)

    button_frame = ttk.Frame(root)
    button_frame.pack(fill="x", padx=10, pady=5)

    refresh_button = ttk.Button(button_frame, text="Atualizar DB", command=lambda: globals().update({'data': refresh_db(tree, columns, display_columns, filters, filtered_stats_label, stat_frame)}))
    refresh_button.pack(side="left", padx=5)

    hashculator_button = ttk.Button(button_frame, text="Hashculator", command=run_hashculator)
    hashculator_button.pack(side="left", padx=5)

    export_button = ttk.Button(button_frame, text="Exportar Playlist (M3U)", command=lambda: export_playlist(tree, columns))
    export_button.pack(side="left", padx=5)

    root.mainloop()

if __name__ == "__main__":
    run_visualization()