import os
import json
import re
import sys

def get_group_key(filename):
    """
    使用正则表达式提取文件的组 key，
    如 "device-udi-0026-of-0044.json" 得到 "device-udi"
    """
    m = re.match(r"^(.*?)-\d+-of-\d+\.json$", filename)
    if m:
        return m.group(1)
    else:
        return filename

def extract_samples_from_json_file(json_path, num_samples=3):
    """
    从 JSON 文件中抽取样例数据：
    保留原始 JSON 结构，仅减少 results 列表中的项目数量
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"无法解析 JSON 文件: {json_path}, 错误: {e}")
        return None

    # 创建原始数据的深拷贝，以避免修改原始数据
    sampled_data = json.loads(json.dumps(data))
    
    # 如果是字典且包含 "results" 字段（为列表），仅减少 results 列表的项目数
    if isinstance(sampled_data, dict) and "results" in sampled_data and isinstance(sampled_data["results"], list):
        sampled_data["results"] = sampled_data["results"][:num_samples]
        return sampled_data
    # 如果是列表，减少列表项目数
    elif isinstance(sampled_data, list):
        return sampled_data[:num_samples]
    # 其他情况保持原样
    else:
        return sampled_data

def process_category(category):
    """
    遍历指定分类（如 device）下所有 JSON 文件，
    先对所有文件（含目录）倒排排序，再按组 key 去重，
    取倒排后每组的第一个文件进行 sample 抽取，
    并将 sample 文件直接保存在 datafiles/samples/<category> 的根目录下。
    """
    source_dir = os.path.join("datafiles", "unzip", category)
    output_dir = os.path.join("datafiles", "samples", category)
    if not os.path.exists(source_dir):
        print(f"源目录不存在: {source_dir}")
        return

    # 收集所有 JSON 文件的完整路径
    all_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".json"):
                full_path = os.path.join(root, file)
                all_files.append(full_path)

    # 对文件路径进行倒排排序（降序）
    all_files.sort(reverse=True)

    # 根据组 key 去重：同一组只保留倒排排序后的第一个文件
    grouped_files = {}
    for file_path in all_files:
        filename = os.path.basename(file_path)
        group_key = get_group_key(filename)
        if group_key not in grouped_files:
            grouped_files[group_key] = file_path
        else:
            print(f"跳过重复文件: {file_path} (组 key: {group_key})")

    # 处理每个去重后的文件
    for group_key, file_path in grouped_files.items():
        print(f"处理文件: {file_path}")
        samples = extract_samples_from_json_file(file_path)
        if samples is None:
            continue

        # 直接保存在输出目录的根目录下，不保留原有目录结构
        base, ext = os.path.splitext(os.path.basename(file_path))
        output_file_name = base + "_sample" + ext
        output_file_path = os.path.join(output_dir, output_file_name)
        os.makedirs(output_dir, exist_ok=True)
        with open(output_file_path, "w", encoding="utf-8") as f_out:
            json.dump(samples, f_out, indent=2, ensure_ascii=False)
        print(f"保存 sample 文件: {output_file_path}")

def main():
    if len(sys.argv) < 2:
        print("请指定目标分类，例如: device")
        return
    category = sys.argv[1]
    process_category(category)
    print("全部 sample 提取完成。")

if __name__ == "__main__":
    main()
