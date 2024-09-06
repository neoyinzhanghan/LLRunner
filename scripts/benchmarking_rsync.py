import os
import time
import shutil
import subprocess
import threading
import psutil
from concurrent.futures import ThreadPoolExecutor

test_slides = [
    "H24-00033-1A-1 - 2024-07-17 13.47.29.ndpi",
    "H24-00033-1A-2 - 2024-07-17 13.49.14.ndpi",
    "H24-00033-1A-2 - 2024-07-17 13.49.15.ndpi",
    "H24-00033-1A-3 - 2024-07-17 13.50.18.ndpi",
    "H24-00033-1A-4 - 2024-07-17 13.51.50.ndpi",
    "H24-00033-1A-5 - 2024-07-17 13.53.43.ndpi",
    "H24-00033-1A-6 - 2024-07-17 13.55.33.ndpi",
    "H24-00033-1A-7 - 2024-07-17 13.57.28.ndpi",
    "H24-00033-2A-10 - 2024-07-17 14.10.06.ndpi",
    "H24-00033-2A-11 - 2024-07-17 14.12.03.ndpi",
    "H24-00033-2A-1 - 2024-07-17 13.59.47.ndpi",
    "H24-00033-2A-1 - 2024-07-17 13.59.48.ndpi",
    "H24-00033-2A-12 - 2024-07-17 14.13.57.ndpi",
    "H24-00033-2A-2 - 2024-07-17 14.01.37.ndpi",
    "H24-00033-2A-3 - 2024-07-17 14.20.15.ndpi",
    "H24-00033-2A-3 - 2024-07-17 14.20.16.ndpi",
    "H24-00033-2A-4 - 2024-07-17 14.19.54.ndpi",
    "H24-00033-2A-5 - 2024-07-17 14.19.32.ndpi",
    "H24-00033-2A-6 - 2024-07-17 14.02.50.ndpi",
    "H24-00033-2A-7 - 2024-07-17 14.04.40.ndpi",
    "H24-00033-2A-8 - 2024-07-17 14.06.19.ndpi",
    "H24-00033-2A-9 - 2024-07-17 14.08.14.ndpi",
    "H24-00034-1A-1 - 2024-07-12 08.44.34.ndpi",
    "H24-00034-1A-1 - 2024-07-16 14.09.21.ndpi",
    "H24-00035-1A-1 - 2024-07-12 08.46.29.ndpi",
    "H24-00035-1A-1 - 2024-07-16 14.09.39.ndpi",
    "H24-00035-2A-1 - 2024-07-12 08.47.11.ndpi",
    "H24-00035-2A-1 - 2024-07-16 14.09.56.ndpi",
    "H24-00035-2A-1 - 2024-07-16 15.44.02.ndpi",
    "H24-1160;S15;MSK3 - 2024-07-10 23.38.36.ndpi",
    "H24-1160;S15;MSK3 - 2024-07-10 23.43.52.ndpi",
    "H24-1160;S15;MSK3 - 2024-07-10 23.53.16.ndpi",
    "H24-1160;S16;MSK4 - 2024-07-10 23.48.25.ndpi",
    "H24-1193;S15;MSKZ - 2024-07-10 23.15.13.ndpi",
    "H24-1193;S15;MSKZ - 2024-07-10 23.19.43.ndpi",
    "H24-1193;S15;MSKZ - 2024-07-10 23.57.50.ndpi",
    "H24-1193;S15;MSKZ - 2024-07-11 00.02.21.ndpi",
    "H24-1193;S16;MSK- - 2024-07-10 23.24.20.ndpi",
    "H24-1311;S11;MSKN - 2024-07-10 22.37.15.ndpi",
    "H24-1311;S11;MSKN - 2024-07-10 23.29.17.ndpi",
    "H24-1311;S11;MSKN - 2024-07-10 23.34.07.ndpi",
    "H24-1311;S12;MSKO - 2024-07-10 22.32.48.ndpi",
    "H24-1508;S13;MSK7 - 2024-07-10 22.47.39.ndpi",
    "H24-1508;S13;MSK7 - 2024-07-10 23.06.04.ndpi",
    "H24-1508;S13;MSK7 - 2024-07-10 23.10.35.ndpi",
    "H24-1508;S17;MSKA - 2024-07-10 22.42.39.ndpi",
    "H24-1514;S15;MSKW - 2024-07-10 22.57.01.ndpi",
    "H24-1514;S15;MSKW - 2024-07-10 23.01.33.ndpi",
    "H24-1514;S15;MSKW - 2024-07-23 22.35.32.ndpi",
    "H24-1514;S15;MSKW - 2024-07-23 22.40.04.ndpi",
    "H24-1514;S17;MSKY - 2024-07-10 22.52.10.ndpi",
    "H24-1514;S17;MSKY - 2024-07-23 22.44.33.ndpi",
    "H24-1896;S15;MSK9 - 2024-07-10 21.48.32.ndpi",
    "H24-1896;S15;MSK9 - 2024-07-10 21.54.13.ndpi",
    "H24-1896;S15;MSK9 - 2024-07-10 22.22.36.ndpi",
    "H24-1896;S15;MSK9 - 2024-07-10 22.28.11.ndpi",
    "H24-1896;S28;MSKC - 2024-07-10 22.18.01.ndpi",
    "H24-2001;S15;MSK8 - 2024-05-13 16.53.08.ndpi",
    "H24-2001;S15;MSK8 - 2024-05-13 16.58.07.ndpi",
    "H24-2001;S16;MSK9 - 2024-05-13 16.48.13.ndpi",
    "H24-2363;S15;MSKZ - 2024-05-13 17.03.00.ndpi",
    "H24-2363;S15;MSKZ - 2024-05-13 17.08.03.ndpi",
    "H24-2363;S16;MSK- - 2024-05-13 17.13.08.ndpi",
    "H24-2522;S11;MSKS - 2024-05-13 12.39.46.ndpi",
    "H24-2522;S11;MSKS - 2024-05-13 13.45.53.ndpi",
    "H24-2522;S11;MSKS - 2024-05-13 14.11.59.ndpi",
    "H24-2522;S11;MSKS - 2024-05-13 17.18.01.ndpi",
    "H24-2522;S12;MSKT - 2024-05-13 13.20.27.ndpi",
    "H24-2591;S11;MSKY - 2024-05-13 14.01.20.ndpi",
    "H24-2591;S11;MSKY - 2024-05-29 05.19.08.ndpi",
    "H24-2591;S11;MSKY - 2024-05-29 05.24.09.ndpi",
    "H24-2591;S11;MSKY - 2024-05-29 05.29.20.ndpi",
    "H24-2591;S13;MSK- - 2024-05-13 11.47.58.ndpi",
    "H24-2796;S13;MSK7 - 2024-07-10 22.03.30.ndpi",
    "H24-2796;S13;MSK7 - 2024-07-10 22.08.07.ndpi",
    "H24-2796;S13;MSK7 - 2024-07-10 22.13.18.ndpi",
    "H24-2796;S14;MSK8 - 2024-07-10 21.58.44.ndpi",
    "H24-3021;S15;MSK1 - 2024-07-10 21.29.52.ndpi",
    "H24-3021;S15;MSK1 - 2024-07-10 21.39.10.ndpi",
    "H24-3021;S16;MSK2 - 2024-07-10 21.34.23.ndpi",
    "H24-3093;S13;MSK8 - 2024-05-20 10.44.31.ndpi",
    "H24-3093;S13;MSK8 - 2024-05-20 12.54.46.ndpi",
    "H24-3093;S13;MSK8 - 2024-05-20 12.59.35.ndpi",
    "H24-3110;S13;MSKY - 2024-05-20 13.04.25.ndpi",
    "H24-3110;S13;MSKY - 2024-05-20 13.09.40.ndpi",
    "H24-3110;S13;MSKY - 2024-05-20 13.29.47.ndpi",
    "H24-3169;S13;MSK2 - 2024-05-20 13.14.56.ndpi",
    "H24-3169;S13;MSK2 - 2024-05-20 13.19.52.ndpi",
    "H24-3169;S13;MSK2 - 2024-05-20 13.34.38.ndpi",
    "H24-3169;S13;MSK2 - 2024-05-20 14.46.55.ndpi",
    "H24-3225;S10;MSK2 - 2024-05-20 11.29.45.ndpi",
    "H24-3225;S10;MSK2 - 2024-05-20 13.24.45.ndpi",
    "H24-3236;S15;MSKZ - 2024-05-20 10.49.22.ndpi",
    "H24-3236;S15;MSKZ - 2024-05-20 10.54.25.ndpi",
    "H24-3243;S10;MSK2 - 2024-05-20 10.59.35.ndpi",
    "H24-3243;S10;MSK2 - 2024-05-20 11.04.31.ndpi",
    "H24-3243;S10;MSK2 - 2024-05-20 11.09.19.ndpi",
    "H24-3265;S11;MSKX - 2024-05-20 11.14.22.ndpi",
    "H24-3265;S11;MSKX - 2024-05-20 11.19.19.ndpi",
    "H24-3265;S11;MSKX - 2024-05-20 11.24.40.ndpi",
    "H24-3268;S9;MSK7 - 2024-05-20 09.43.27.ndpi",
    "H24-3268;S9;MSK7 - 2024-05-20 09.48.34.ndpi",
    "H24-3268;S9;MSK7 - 2024-05-20 13.44.30.ndpi",
    "H24-3273;S13;MSKY - 2024-05-20 09.53.30.ndpi",
    "H24-3273;S13;MSKY - 2024-05-20 09.58.23.ndpi",
    "H24-3273;S13;MSKY - 2024-05-20 14.19.27.ndpi",
    "H24-3273;S13;MSKY - 2024-05-20 14.59.21.ndpi",
    "H24-3280;S17;MSK0 - 2024-05-20 10.29.07.ndpi",
    "H24-3280;S17;MSK0 - 2024-05-20 10.34.22.ndpi",
    "H24-3280;S18;MSKA - 2024-05-20 10.08.32.ndpi",
    "H24-3280;S18;MSKA - 2024-05-20 10.13.45.ndpi",
    "H24-3280;S19;MSKB - 2024-05-20 10.03.31.ndpi",
    "H24-3280;S19;MSKB - 2024-05-20 11.44.56.ndpi",
    "H24-3280;S20;MSKD - 2024-05-20 10.18.44.ndpi",
    "H24-3280;S20;MSKD - 2024-05-20 10.23.58.ndpi",
    "H24-3359;S13;MSK3 - 2024-05-20 09.23.50.ndpi",
    "H24-3359;S13;MSK3 - 2024-05-20 10.39.20.ndpi",
    "H24-3359;S13;MSK3 - 2024-05-20 11.49.50.ndpi",
    "H24-3436;S13;MSKZ - 2024-05-20 09.28.40.ndpi",
    "H24-3436;S13;MSKZ - 2024-05-20 09.33.42.ndpi",
    "H24-3436;S13;MSKZ - 2024-05-20 09.38.37.ndpi",
    "H24-3456;S17;MSK5 - 2024-05-20 12.29.35.ndpi",
    "H24-3456;S17;MSK5 - 2024-05-20 12.34.34.ndpi",
    "H24-3456;S18;MSK6 - 2024-05-20 12.04.54.ndpi",
    "H24-3456;S18;MSK6 - 2024-05-20 12.19.45.ndpi",
    "H24-3456;S19;MSK7 - 2024-05-20 12.24.40.ndpi",
    "H24-3456;S19;MSK7 - 2024-05-20 20.33.57.ndpi",
    "H24-3456;S20;MSK9 - 2024-05-20 12.09.50.ndpi",
    "H24-3456;S20;MSK9 - 2024-05-20 12.14.45.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 11.54.45.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 12.00.04.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 12.39.28.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 12.44.44.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 12.49.46.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 13.39.28.ndpi",
    "H24-3462;S13;MSKY - 2024-05-20 14.51.58.ndpi",
    "H24-3462;S14;MSKZ - 2024-07-10 21.43.49.ndpi",
    "H24-3483;S14;MSK2 - 2024-05-20 11.34.55.ndpi",
    "H24-3483;S14;MSK2 - 2024-05-20 11.40.00.ndpi",
    "H24-3534;S17;MSK2 - 2024-05-20 18.26.41.ndpi",
    "H24-3534;S17;MSK2 - 2024-05-20 18.31.34.ndpi",
    "H24-3534;S18;MSK3 - 2024-05-20 18.46.29.ndpi",
    "H24-3534;S18;MSK3 - 2024-05-20 18.51.35.ndpi",
    "H24-3534;S19;MSK4 - 2024-05-20 18.36.29.ndpi",
    "H24-3534;S19;MSK4 - 2024-05-20 18.56.43.ndpi",
    "H24-3534;S19;MSK4 - 2024-05-20 23.33.17.ndpi",
    "H24-3534;S20;MSK6 - 2024-05-20 18.41.36.ndpi",
    "H24-3534;S20;MSK6 - 2024-05-20 19.01.37.ndpi",
    "H24-3562;S15;MSK1 - 2024-05-20 20.38.51.ndpi",
    "H24-3562;S15;MSK1 - 2024-05-20 20.44.01.ndpi",
    "H24-3583;S13;MSK2 - 2024-05-20 21.40.30.ndpi",
    "H24-3583;S13;MSK2 - 2024-05-20 21.45.45.ndpi",
    "H24-3583;S13;MSK2 - 2024-05-20 21.50.36.ndpi",
    "H24-3599;S17;MSKC - 2024-05-20 22.05.40.ndpi",
    "H24-3599;S17;MSKC - 2024-05-20 22.15.49.ndpi",
    "H24-3599;S18;MSKD - 2024-05-20 19.53.23.ndpi",
    "H24-3599;S18;MSKD - 2024-05-20 22.25.56.ndpi",
    "H24-3599;S19;MSKE - 2024-05-20 21.55.38.ndpi",
    "H24-3599;S19;MSKE - 2024-05-20 22.00.37.ndpi",
    "H24-3599;S20;MSKG - 2024-05-20 22.10.50.ndpi",
    "H24-3599;S20;MSKG - 2024-05-20 22.20.44.ndpi",
    "H24-3604;S13;MSK6 - 2024-05-20 19.38.28.ndpi",
    "H24-3604;S13;MSK6 - 2024-05-20 19.43.31.ndpi",
    "H24-3604;S13;MSK6 - 2024-05-20 19.48.24.ndpi",
    "H24-3625;S13;MSKZ - 2024-05-20 19.11.37.ndpi",
    "H24-3625;S13;MSKZ - 2024-05-20 19.17.23.ndpi",
    "H24-3625;S13;MSKZ - 2024-05-20 19.22.45.ndpi",
    "H24-3625;S13;MSKZ - 2024-05-20 19.28.14.ndpi",
    "H24-3625;S13;MSKZ - 2024-05-20 19.33.33.ndpi",
    "H24-3630;S11;MSK3 - 2024-05-20 19.06.38.ndpi",
    "H24-3630;S11;MSK3 - 2024-05-20 21.30.38.ndpi",
    "H24-3630;S11;MSK3 - 2024-05-20 21.35.38.ndpi",
    "H24-3660;S10;MSKE - 2024-05-20 21.20.51.ndpi",
    "H24-3660;S10;MSKE - 2024-05-20 21.25.45.ndpi",
    "H24-3673;S10;MSK9 - 2024-05-20 21.04.59.ndpi",
    "H24-3673;S10;MSK9 - 2024-05-20 21.10.38.ndpi",
    "H24-3673;S10;MSK9 - 2024-05-20 21.15.34.ndpi",
    "H24-3691;S12;MSK1 - 2024-05-20 20.49.08.ndpi",
    "H24-3691;S12;MSK1 - 2024-05-20 20.54.03.ndpi",
    "H24-3691;S12;MSK1 - 2024-05-20 20.59.52.ndpi",
    "H24-3707;S13;MSK0 - 2024-05-20 19.58.32.ndpi",
    "H24-3707;S13;MSK0 - 2024-05-20 20.03.31.ndpi",
    "H24-3707;S13;MSK0 - 2024-05-20 20.08.35.ndpi",
    "H24-3710;S10;MSKA - 2024-05-20 20.13.43.ndpi",
    "H24-3710;S10;MSKA - 2024-05-20 20.18.39.ndpi",
    "H24-375;S12;MSKX - 2024-05-13 11.17.54.ndpi",
    "H24-375;S12;MSKX - 2024-05-13 12.03.06.ndpi",
    "H24-375;S12;MSKX - 2024-05-13 12.08.08.ndpi",
    "H24-375;S13;MSKY - 2024-05-13 11.12.57.ndpi",
    "H24-3785;S11;MSK4 - 2024-05-20 20.23.51.ndpi",
    "H24-3785;S11;MSK4 - 2024-05-20 20.28.50.ndpi",
    "H24-513;S15;MSKU - 2024-05-13 11.23.08.ndpi",
    "H24-513;S15;MSKU - 2024-05-13 13.36.05.ndpi",
    "H24-513;S15;MSKU - 2024-05-13 13.40.59.ndpi",
    "H24-513;S16;MSKV - 2024-05-13 13.25.37.ndpi",
    "H24-513;S16;MSKV - 2024-05-13 13.30.48.ndpi",
    "H24-53;S14;MSKS - 2024-05-13 11.33.15.ndpi",
    "H24-53;S14;MSKS - 2024-05-13 11.38.07.ndpi",
    "H24-53;S14;MSKS - 2024-05-13 11.43.06.ndpi",
    "H24-53;S14;MSKS - 2024-06-11 18.45.11.ndpi",
    "H24-53;S14;MSKS - 2024-06-11 18.50.02.ndpi",
    "H24-53;S14;MSKS - 2024-06-25 11.14.00.ndpi",
    "H24-53;S16;MSKU - 2024-05-13 11.28.01.ndpi",
    "H24-53;S16;MSKU - 2024-06-11 18.39.53.ndpi",
    "H24-736;S11;MSKX - 2024-05-13 11.08.03.ndpi",
    "H24-737;S11;MSKY - 2024-05-13 12.29.33.ndpi",
    "H24-737;S11;MSKY - 2024-05-13 12.34.37.ndpi",
    "H24-737;S12;MSKZ - 2024-05-13 11.02.53.ndpi",
]


# List of test slides filenames
test_slides = test_slides[:32]

source_dir = "/pesgisipth/NDPI"
destination_dir = "/media/hdd3/neo/test_slides_rsync"


# Rsync function for a single file
def rsync_file(file):
    source_file = os.path.join(source_dir, file)
    destination_file = os.path.join(destination_dir, file)

    try:
        subprocess.run(
            ["rsync", "-av", source_file, destination_file],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error rsyncing {file}: {e}")


# Function to measure rsync time and peak CPU usage with different numbers of workers
def measure_rsync_time_and_cpu(num_workers):
    print(f"Measuring rsync time and CPU usage with {num_workers} workers...")
    peak_cpu_usage = [0]  # Using a list to allow modification inside nested function
    stop_monitoring = [False]

    # Function to monitor CPU usage
    def monitor_cpu_usage():
        while not stop_monitoring[0]:
            # Measure CPU usage over all cores
            cpu_usage = psutil.cpu_percent(interval=0.1)
            if cpu_usage > peak_cpu_usage[0]:
                peak_cpu_usage[0] = cpu_usage

    # Start CPU monitoring thread
    cpu_monitor_thread = threading.Thread(target=monitor_cpu_usage)
    cpu_monitor_thread.start()

    # Start rsync operations
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(rsync_file, test_slides)
    end_time = time.time()

    # Stop CPU monitoring
    stop_monitoring[0] = True
    cpu_monitor_thread.join()

    elapsed_time = end_time - start_time
    return elapsed_time, peak_cpu_usage[0]

    print(f"Time taken with {num_workers} workers: {elapsed_time:.2f} seconds")
    print(f"Peak CPU usage with {num_workers} workers: {peak_cpu_usage[0]:.2f}%")


# List of num_rsync_workers to test
num_rsync_workers_list = [1, 2, 4, 8, 16, 32]

# Measure and print time and peak CPU usage for each worker configuration
for num_workers in num_rsync_workers_list:
    elapsed_time, peak_cpu = measure_rsync_time_and_cpu(num_workers)
    print(f"Time taken with {num_workers} workers: {elapsed_time:.2f} seconds")
    print(f"Peak CPU usage with {num_workers} workers: {peak_cpu:.2f}%\n")
