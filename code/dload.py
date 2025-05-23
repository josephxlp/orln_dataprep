import subprocess
import os
from concurrent.futures import ThreadPoolExecutor

def download_file(url, output_dir):
    """Download a file using wget."""
    try:
        subprocess.run(["wget", "-P", output_dir, url], check=True)
        print(f"Downloaded: {url}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to download {url}: {e}")

def read_txt(file_path):
    """Read URLs from a TXT file."""
    urls = []
    with open(file_path, "r", encoding="utf-8") as txtfile:
        for line in txtfile:
            url = line.strip()
            if url:  # Ensure it's not an empty line
                urls.append(url)
    return urls

def main(txt_file, output_dir, max_workers=5):
    """Main function to manage parallel downloads."""
    os.makedirs(output_dir, exist_ok=True)
    urls = read_txt(txt_file)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda url: download_file(url, output_dir), urls)
    
    print("All downloads completed.")

if __name__ == "__main__":
    csv_file = "lazurls.txt"  # Change to your CSV file path
    output_dir = "/media/ljp238/12TBWolf/ARCHIEVE/Brazil_ORNL_laz/"  # Change to your preferred output directory
    max_workers = 10  # Adjust based on your bandwidth and system capacity
    main(csv_file, output_dir, max_workers)
