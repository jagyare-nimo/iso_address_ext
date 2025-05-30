import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading


class UnixLFConverter:
    def __init__(self, num_worker_threads: int = None, chunk_size_lines: int = 10000):
        self.num_worker_threads = num_worker_threads if num_worker_threads is not None else (os.cpu_count() or 1) * 2
        self.chunk_size_lines = chunk_size_lines
        print(
            f"Converter initialized with {self.num_worker_threads} worker threads and chunk size of {self.chunk_size_lines} lines.")

    def _process_chunk(self, chunk_id: int, lines: list[str]) -> tuple[int, bytes]:
        processed_content = "\n".join(
            line.rstrip('\r\n') for line in lines) + "\n"  # Ensure single LF and trailing newline
        return chunk_id, processed_content.encode('utf-8')

    def convert_csv_to_text(self, input_csv_path: str, output_text_path: str) -> bool:
        input_path = Path(input_csv_path)
        output_path = Path(output_text_path)

        if not input_path.exists():
            print(f"Error: Input file not found at '{input_path}'")
            return False
        if not input_path.is_file():
            print(f"Error: Input path '{input_path}' is not a file.")
            return False
        if input_path.suffix.lower() not in ('.csv', '.txt'):
            print(
                f"Warning: Input file '{input_path}' does not have a typical .csv or .txt extension. Proceeding anyway.")

        output_dir = output_path.parent
        if output_dir and not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                print(f"Error: Could not create output directory '{output_dir}'. Reason: {e}")
                return False

        processed_chunks_queue = queue.Queue()
        expected_chunk_id = 0
        conversion_successful = True

        stop_writer_event = threading.Event()

        def writer_thread_func():
            nonlocal expected_chunk_id, conversion_successful
            try:
                with open(output_path, 'wb') as outfile:
                    while True:
                        try:
                            chunk_id, processed_bytes = processed_chunks_queue.get(timeout=1)

                            if chunk_id == expected_chunk_id:
                                outfile.write(processed_bytes)
                                expected_chunk_id += 1
                            else:
                                processed_chunks_queue.put((chunk_id, processed_bytes))
                                threading.sleep(0.01)

                            processed_chunks_queue.task_done()
                        except queue.Empty:
                            if stop_writer_event.is_set():
                                break
                        except Exception as e:
                            print(f"Error in writer thread: {e}")
                            conversion_successful = False
                            break
            except Exception as e:
                print(f"Error opening output file for writing: {e}")
                conversion_successful = False
            finally:
                while not processed_chunks_queue.empty():
                    processed_chunks_queue.task_done()

        writer_thread = threading.Thread(target=writer_thread_func, daemon=True)
        writer_thread.start()

        try:
            with ThreadPoolExecutor(max_workers=self.num_worker_threads) as executor:
                futures = []
                chunk_id_counter = 0
                current_chunk_lines = []

                with open(input_path, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        current_chunk_lines.append(line.rstrip('\n'))
                        if len(current_chunk_lines) >= self.chunk_size_lines:
                            futures.append(executor.submit(self._process_chunk, chunk_id_counter, current_chunk_lines))
                            chunk_id_counter += 1
                            current_chunk_lines = []

                    if current_chunk_lines:
                        futures.append(executor.submit(self._process_chunk, chunk_id_counter, current_chunk_lines))
                        chunk_id_counter += 1

                for future in as_completed(futures):
                    try:
                        chunk_id, processed_bytes = future.result()
                        processed_chunks_queue.put((chunk_id, processed_bytes))
                    except Exception as exc:
                        print(f"Chunk processing generated an exception: {exc}")
                        conversion_successful = False
                        break  # Stop if a chunk fails

        except UnicodeDecodeError:
            print(f"Error: Could not decode '{input_path}'. It might not be UTF-8 encoded. "
                  "Please ensure the input file is UTF-8 or specify the correct encoding.")
            conversion_successful = False
        except IOError as e:
            print(f"Error during file reading: {e}")
            conversion_successful = False
        except Exception as e:
            print(f"An unexpected error occurred during conversion setup: {e}")
            conversion_successful = False
        finally:
            processed_chunks_queue.join()
            stop_writer_event.set()
            writer_thread.join(timeout=5)

            if writer_thread.is_alive():
                print("Warning: Writer thread did not terminate gracefully.")
                conversion_successful = False

        if conversion_successful:
            print(f"Successfully converted '{input_path}' to '{output_path}' with UTF-8 encoding and UNIX LF format.")
        else:
            print(f"Conversion of '{input_path}' to '{output_path}' failed.")

        return conversion_successful
