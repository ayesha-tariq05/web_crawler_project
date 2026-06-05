import matplotlib.pyplot as plt

# =========================
# INPUT DATA
# =========================

threads = [16, 20, 30, 40]

chunk_size = [30, 50, 100, 120]

download_time = [73.8654, 70.4970, 77.1680, 80.4023]
parse_time = [0.0009, 0.0009, 0.0015, 0.0013]
aggregator_time = [0.0, 0.0, 0.0, 0.0]
total_time = [73.8662, 70.4979, 77.1696, 80.4035]
throughput = [26.87, 37.49, 61.31, 54.54]

# =========================
# GRAPH 1: Threads vs Execution Time
# =========================
plt.figure()
plt.plot(threads, total_time, marker='o')
plt.title("Threads vs Execution Time")
plt.xlabel("Threads (T)")
plt.ylabel("Execution Time (sec)")
plt.grid()
plt.show()

# =========================
# GRAPH 2: Chunk Size vs Throughput
# =========================
plt.figure()
plt.plot(chunk_size, throughput, marker='o')
plt.title("Chunk Size vs Throughput")
plt.xlabel("Chunk Size (N)")
plt.ylabel("Throughput (words/sec)")
plt.grid()
plt.show()


# =========================
# D:P Ratios
# =========================

dp_ratios = [
    "1/9",
    "1/3",
    "1",
    "3",
    "9"
]

# =========================
# Execution Times
# Replace these with YOUR actual values
# =========================

execution_time = [
    78.5,   # D=2,  P=18
    65.2,   # D=5,  P=15
    52.8,   # D=10, P=10
    60.4,   # D=15, P=5
    74.1    # D=18, P=2
]

# =========================
# GRAPH
# =========================

plt.figure()

plt.plot(
    dp_ratios,
    execution_time,
    marker='o'
)

plt.title("D:P Ratio vs Execution Time")

plt.xlabel("D:P Ratio")

plt.ylabel("Execution Time (sec)")

plt.grid()

plt.show()