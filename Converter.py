import os, ujson
from concurrent.futures import ProcessPoolExecutor

def Json_File_Directory(folder_path, output_file):
    with open(output_file, "w", encoding="utf-8") as outfile:
        for filename in os.listdir(folder_path):
            if not filename.endswith(".json"): continue
            input_path = os.path.join(folder_path, filename)
            try:
                with open(input_path, "r", encoding="utf-8") as infile:
                    data = ujson.load(infile)
            except Exception as e:
                print(f"Skipping {filename} in {folder_path}, error: {e}")
                continue
            data = data if isinstance(data, list) else [data]
            for item in data:
                outfile.write(ujson.dumps(item, ensure_ascii=False) + "\n")
            outfile.flush()
        print(f"Created {output_file}")

def Jsonl_Output_Directory(parent_folder, output_folder, workers=4):
    os.makedirs(output_folder, exist_ok=True)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        tasks = [executor.submit(Json_File_Directory,
                 os.path.join(parent_folder, folder),
                 os.path.join(output_folder, f"{folder}.jsonl"))
                 for folder in os.listdir(parent_folder)
                 if os.path.isdir(os.path.join(parent_folder, folder))]
        for task in tasks: task.result()
    print("All folders processed!")

if __name__ == "__main__":
    parent_directory = "Replace_Me"
    output_directory = "Replace_Me"
    Jsonl_Output_Directory(parent_directory, output_directory, workers=8)
