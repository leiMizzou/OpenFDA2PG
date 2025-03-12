import os
import sys
import requests
import zipfile

# 顶层目录
ZIP_FOLDER = "datafiles/zip"
UNZIP_FOLDER = "datafiles/unzip"

def ensure_directory(path: str):
    os.makedirs(path, exist_ok=True)

def ensure_base_directories():
    ensure_directory(ZIP_FOLDER)
    ensure_directory(UNZIP_FOLDER)

def download_with_resume(url: str, save_as: str, chunk_size=8192, max_retries=3):
    import traceback
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            # HEAD 获取文件大小及断点续传信息
            head_resp = requests.head(url, allow_redirects=True)
            if head_resp.status_code != 200:
                raise Exception(f"HEAD 请求失败，状态码: {head_resp.status_code}")
            content_length = int(head_resp.headers.get("Content-Length", 0))
            accept_ranges = head_resp.headers.get("Accept-Ranges", "")
            # 如果文件已存在且大小一致，则跳过
            if os.path.exists(save_as):
                local_size = os.path.getsize(save_as)
                if local_size == content_length:
                    print(f"文件已完整下载，跳过: {save_as}")
                    return
                # 断点续传
                if local_size < content_length and "bytes" in accept_ranges.lower():
                    print(f"检测到部分文件，断点续传: {save_as}，已下载 {local_size}/{content_length} 字节.")
                    headers = {"Range": f"bytes={local_size}-"}
                    resp = requests.get(url, headers=headers, stream=True)
                    resp.raise_for_status()
                    with open(save_as, "ab") as f:
                        for chunk in resp.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                    print(f"断点续传完成: {save_as}")
                    return
                else:
                    print(f"服务器不支持续传或文件异常，删除后重新下载: {save_as}")
                    os.remove(save_as)
            # 完整下载
            print(f"开始完整下载 (attempt {attempt}/{max_retries}): {url} -> {save_as}")
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(save_as, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            print(f"下载完成: {save_as}")
            return
        except Exception as e:
            print(f"下载出现异常: {e}")
            if attempt < max_retries:
                print("准备重试...")
            else:
                raise

def download_and_unzip(url: str, save_as: str, extract_to: str):
    download_with_resume(url, save_as)
    try:
        with zipfile.ZipFile(save_as, "r") as zf:
            zf.extractall(extract_to)
        print(f"解压完成: {save_as} -> {extract_to}")
    except zipfile.BadZipFile:
        print(f"解压失败，文件损坏或非 ZIP: {save_as}")

def process_category(main_cat: str, sub_cat: str, category_info: dict, filter_str: str = None):
    partitions = category_info.get("partitions", [])
    if not partitions:
        print(f"\n分类 {main_cat}/{sub_cat} 下无 partitions")
        return
    print(f"\n开始处理分类: {main_cat}/{sub_cat}")
    for part in partitions:
        display_name = part.get("display_name", "unknown")
        if filter_str and filter_str not in display_name:
            continue
        zip_url = part.get("file")
        if not zip_url:
            print(f"无下载链接: {display_name}")
            continue

        # 对于 event 分类（或其他需要按子目录分组的情况），从 URL 提取倒数第二个部分作为子目录
        if sub_cat == "event":
            splitted = zip_url.split("/")
            if len(splitted) >= 2:
                subfolder = splitted[-2]
            else:
                subfolder = "unknown"
            zip_folder_cat = os.path.join(ZIP_FOLDER, main_cat, sub_cat, subfolder)
            unzip_folder_cat = os.path.join(UNZIP_FOLDER, main_cat, sub_cat, subfolder)
        else:
            zip_folder_cat = os.path.join(ZIP_FOLDER, main_cat, sub_cat)
            unzip_folder_cat = os.path.join(UNZIP_FOLDER, main_cat, sub_cat)

        ensure_directory(zip_folder_cat)
        ensure_directory(unzip_folder_cat)

        zip_filename = os.path.basename(zip_url)
        local_zip_path = os.path.join(zip_folder_cat, zip_filename)
        download_and_unzip(zip_url, local_zip_path, unzip_folder_cat)

def main():
    ensure_base_directories()
    # 获取下载索引，此处仍采用 openFDA 的下载索引地址
    download_json_url = "https://api.fda.gov/download.json"
    print(f"请求下载索引: {download_json_url}")
    resp = requests.get(download_json_url)
    resp.raise_for_status()
    data = resp.json()

    # 用户需要通过命令行指定目标分类，格式为 "主分类/子分类"，例如 "device/udi" 或 "food/enforcement"
    if len(sys.argv) < 2:
        print("请指定目标分类，例如：device/udi 或 food/event")
        return
    target = sys.argv[1]

    # 可选：传入过滤关键字，例如只下载 display_name 中包含 "2024" 的分区
    filter_str = sys.argv[2] if len(sys.argv) >= 3 else None

    if "/" in target:
        main_cat, sub_cat = target.split("/", 1)
    else:
        main_cat = target
        sub_cat = None

    results = data.get("results", {})
    if main_cat not in results:
        print(f"错误：未找到主分类 {main_cat}")
        return

    category_data = results[main_cat]
    if sub_cat:
        if sub_cat not in category_data:
            print(f"错误：在 {main_cat} 分类下未找到子分类 {sub_cat}")
            return
        category_info = category_data[sub_cat]
        process_category(main_cat, sub_cat, category_info, filter_str)
    else:
        # 如果只指定了主分类，则遍历其下所有子分类
        for sub_cat_key, cat_info in category_data.items():
            process_category(main_cat, sub_cat_key, cat_info, filter_str)

    print("\n全部下载/解压任务完成。")

if __name__ == "__main__":
    main()
