import os
import json
import csv
import threading
import shlex
from queue import Queue

# 설정값
OUTPUT_CSV_FILE = "matching_files.csv"
BATCH_SIZE = 500  # 배치 크기
NUM_THREADS = 8   # 동시에 실행할 쓰레드 개수

def find_matching_files_in_batch(file_batch, folder_path, search_values, result_queue, index, total_batches):
    """배치 단위로 JSON 파일을 검색하고, 특정 값을 포함하는 파일을 찾음"""
    batch_results = []
    for filename in file_batch:
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            matched_values = extract_matching_values(data, search_values)
            if matched_values:
                for value in matched_values:
                    batch_results.append([filename, file_path, value])
        except Exception as e:
            print(f"⚠️ 파일 {filename} 읽기 오류: {e}")

    result_queue.put(batch_results)
    
    # 진행률 출력
    progress = ((index + 1) / total_batches) * 100
    print(f"📂 진행률: {progress:.2f}% ({index + 1}/{total_batches} 배치 처리 완료)", end="\r")

def extract_matching_values(data, search_values):
    """JSON 데이터에서 특정 값을 찾아 반환"""
    matched_values = set()
    
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                matched_values.update(extract_matching_values(value, search_values))
            elif isinstance(value, str) and value in search_values:
                matched_values.add(value)
    
    elif isinstance(data, list):
        for item in data:
            matched_values.update(extract_matching_values(item, search_values))

    return matched_values

def find_matching_files_multithreaded(folder_path, search_values):
    """멀티스레드로 JSON 파일을 검색"""
    matching_files = []
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
    total_files = len(json_files)

    if total_files == 0:
        print("❌ 선택한 폴더 내 JSON 파일이 없습니다.")
        return

    # 파일을 배치 단위로 나누기
    file_batches = [json_files[i:i + BATCH_SIZE] for i in range(0, total_files, BATCH_SIZE)]
    total_batches = len(file_batches)

    print(f"\n🔍 총 {total_files}개의 JSON 파일을 {total_batches}개 배치로 검색 중...\n")

    result_queue = Queue()
    threads = []

    # 배치별로 멀티스레딩 실행
    for index, file_batch in enumerate(file_batches):
        thread = threading.Thread(target=find_matching_files_in_batch, args=(file_batch, folder_path, search_values, result_queue, index, total_batches))
        thread.start()
        threads.append(thread)

    # 모든 쓰레드가 종료될 때까지 대기
    for thread in threads:
        thread.join()

    # 결과 취합
    while not result_queue.empty():
        matching_files.extend(result_queue.get())

    # CSV 파일로 저장
    if matching_files:
        with open(OUTPUT_CSV_FILE, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["파일 이름", "파일 경로", "검색된 값"])  # 헤더 추가
            writer.writerows(matching_files)

        print(f"\n✅ 총 {len(matching_files)}개의 JSON 파일에서 지정된 값을 찾았습니다!")
        print(f"📂 결과가 '{OUTPUT_CSV_FILE}' 파일로 저장되었습니다.")
    else:
        print("\n❌ 지정된 값을 포함하는 JSON 파일을 찾지 못했습니다.")

def main():
    """사용자로부터 폴더 경로와 검색할 값을 입력받고 JSON 파일 탐색"""
    folder_path = input("📂 JSON 파일이 들어있는 폴더 경로를 입력하세요: ").strip()
    if not os.path.exists(folder_path):
        print("\n❌ 해당 폴더가 존재하지 않습니다. 경로를 확인하세요.")
        return

    # 검색할 값 입력받기 (쉼표가 포함된 값도 올바르게 처리)
    search_input = input('🔍 찾고자 하는 값을 **"값1" "값2" "값3"** 형식으로 입력하세요: ').strip()
    search_values = set(shlex.split(search_input))  # 따옴표로 감싼 값을 올바르게 분리

    if not search_values:
        print("\n❌ 검색할 값이 없습니다. 다시 실행해주세요.")
        return

    find_matching_files_multithreaded(folder_path, search_values)

# 실행
if __name__ == "__main__":
    main()
