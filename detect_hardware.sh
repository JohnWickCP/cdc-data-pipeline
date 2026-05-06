#!/bin/bash
# ============================================================
# detect_hardware.sh — Phát hiện phần cứng, đề xuất profile
#
# Hỗ trợ: Windows (Git Bash / MSYS2) + Linux
# Graceful fallback nếu thiếu quyền — Linux tự hỏi sudo pass
#
# Usage:
#   bash detect_hardware.sh          # chạy bình thường
#   sudo bash detect_hardware.sh     # Linux: đọc đầy đủ DMI/VM info
# ============================================================

export MSYS_NO_PATHCONV=1

# ── Colors ────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'
B='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

section() { echo -e "\n${BOLD}${B}══ $1 ══${NC}"; }
ok()      { echo -e "  ${G}✓${NC} $1"; }
warn()    { echo -e "  ${Y}⚠${NC} $1"; }
note()    { echo -e "  ${DIM}$1${NC}"; }

# ── OS Detection ──────────────────────────────────────────
detect_os() {
    local k
    k=$(uname -s 2>/dev/null || echo "unknown")
    case "$k" in
        Linux*)        echo "linux"   ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        Darwin*)       echo "macos"   ;;
        *)             echo "unknown" ;;
    esac
}

OS=$(detect_os)

# ── Privilege Detection & Escalation ──────────────────────
ELEVATED=false

if [ "$OS" = "linux" ]; then
    [ "$(id -u)" = "0" ] && ELEVATED=true
elif [ "$OS" = "windows" ]; then
    # Thử net session trước (nhanh, không cần PowerShell)
    if net session > /dev/null 2>&1; then
        ELEVATED=true
    else
        # Fallback: hỏi PowerShell
        _is_admin=$(powershell -NoProfile -Command \
            "([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)" \
            2>/dev/null | tr -d '\r\n')
        [ "$_is_admin" = "True" ] && ELEVATED=true
    fi
fi

# Linux không phải root → hỏi có muốn sudo không
if [ "$OS" = "linux" ] && [ "$ELEVATED" = "false" ]; then
    echo ""
    echo -e "${Y}⚠  Đang chạy không có quyền root.${NC}"
    echo -e "   Một số thông tin (VM vendor, DMI) cần root để chính xác hơn."
    echo -ne "   ${BOLD}Chạy lại với sudo không? (y/N): ${NC}"
    read -r _sudo_ans
    if [ "$_sudo_ans" = "y" ] || [ "$_sudo_ans" = "Y" ]; then
        # exec thay thế process hiện tại → sudo tự hỏi password
        exec sudo bash "$0" "$@"
    fi
    echo ""
fi

# Windows không phải admin → thông báo (không thể auto-relaunch trong Git Bash)
if [ "$OS" = "windows" ] && [ "$ELEVATED" = "false" ]; then
    echo ""
    echo -e "${Y}⚠  Đang chạy không có quyền Administrator.${NC}"
    echo -e "   Thông tin RAM và VM detection có thể bị thiếu."
    echo -e "   ${DIM}→ Để chạy đầy đủ: click phải Git Bash → 'Run as Administrator'${NC}"
    echo ""
fi

# ── Helpers ───────────────────────────────────────────────

# Chạy wmic, trả về giá trị sau dấu = hoặc ""
# wmic đôi khi cần admin; nếu lỗi trả về ""
wmic_val() {
    local key="$1" cmd="$2"
    local out
    out=$(eval "$cmd" 2>/dev/null | tr -d '\r' | grep "^${key}=" | cut -d= -f2- | xargs || echo "")
    echo "$out"
}

# Fallback Windows: dùng PowerShell nếu wmic thất bại
ps_val() {
    local expr="$1"
    powershell -NoProfile -Command "$expr" 2>/dev/null | tr -d '\r\n' || echo ""
}

# ── CPU Model ─────────────────────────────────────────────
detect_cpu_model() {
    if [ "$OS" = "windows" ]; then
        local m
        m=$(wmic_val "Name" "wmic cpu get Name /value")
        [ -z "$m" ] && m=$(ps_val "(Get-WmiObject Win32_Processor).Name")
        echo "${m:-N/A}"
    else
        local m
        m=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | sed 's/.*:\s*//' | xargs || echo "")
        [ -z "$m" ] && m=$(lscpu 2>/dev/null | grep 'Model name' | sed 's/.*:\s*//' | xargs || echo "")
        echo "${m:-N/A}"
    fi
}

# ── CPU Cores (logical) ───────────────────────────────────
detect_cpu_cores() {
    if [ "$OS" = "windows" ]; then
        local c
        c=$(wmic_val "NumberOfLogicalProcessors" "wmic cpu get NumberOfLogicalProcessors /value")
        [ -z "$c" ] && c=$(ps_val "(Get-WmiObject Win32_Processor).NumberOfLogicalProcessors")
        [ -z "$c" ] && c=$(nproc 2>/dev/null || echo "0")
        echo "$c"
    else
        nproc 2>/dev/null || grep -c '^processor' /proc/cpuinfo 2>/dev/null || echo "0"
    fi
}

# ── RAM (GB) ──────────────────────────────────────────────
detect_ram_gb() {
    if [ "$OS" = "windows" ]; then
        local bytes
        bytes=$(wmic_val "TotalPhysicalMemory" "wmic computersystem get TotalPhysicalMemory /value")
        if [ -z "$bytes" ]; then
            bytes=$(ps_val "(Get-WmiObject Win32_ComputerSystem).TotalPhysicalMemory")
        fi
        if [[ "$bytes" =~ ^[0-9]+$ ]] && [ "$bytes" -gt 0 ]; then
            echo $(( bytes / 1024 / 1024 / 1024 ))
        else
            echo "0"
        fi
    elif [ "$OS" = "linux" ]; then
        local mb
        mb=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "0")
        echo $(( mb / 1024 ))
    else
        echo "0"
    fi
}

# ── Battery — laptop indicator ─────────────────────────────
# Không cần quyền admin trên cả Windows lẫn Linux
detect_battery() {
    if [ "$OS" = "windows" ]; then
        # BatteryStatus: 1=Discharging(trên pin), 2=AC, blank=không có pin
        local s
        s=$(wmic_val "BatteryStatus" "wmic path Win32_Battery get BatteryStatus /value")
        [ -n "$s" ] && echo "yes" || echo "no"
    else
        # /sys/class/power_supply/BAT* tồn tại = có pin; không cần root
        if ls /sys/class/power_supply/ 2>/dev/null | grep -qi "bat"; then
            echo "yes"
        else
            echo "no"
        fi
    fi
}

# ── Disk Free ─────────────────────────────────────────────
detect_disk_free() {
    df -h "${1:-.}" 2>/dev/null | tail -1 | awk '{print $4}' || echo "N/A"
}

# ── Virtualization ────────────────────────────────────────
# Windows: wmic không cần admin để đọc model/manufacturer
# Linux:   systemd-detect-virt thường không cần root;
#          /proc/cpuinfo hypervisor flag cũng không cần root;
#          /sys/class/dmi/id/* có thể cần root → fallback gracefully
detect_virt() {
    if [ "$OS" = "windows" ]; then
        local model mfr combined
        model=$(wmic_val "Model"        "wmic computersystem get model /value")
        mfr=$(wmic_val   "Manufacturer" "wmic computersystem get manufacturer /value")
        combined=$(echo "${model} ${mfr}" | tr '[:upper:]' '[:lower:]')
        case "$combined" in
            *virtualbox*)          echo "VirtualBox" ;;
            *vmware*)              echo "VMware"     ;;
            *"hyper-v"*|*"virtual machine"*) echo "Hyper-V" ;;
            *kvm*|*qemu*)          echo "KVM/QEMU"  ;;
            *)                     echo "none"       ;;
        esac

    else
        # 1) systemd-detect-virt — thường không cần root
        local virt
        virt=$(systemd-detect-virt 2>/dev/null || echo "")
        if [ -n "$virt" ] && [ "$virt" != "none" ]; then
            echo "$virt"
            return
        fi

        # 2) /proc/cpuinfo hypervisor flag — không cần root
        if grep -q "hypervisor" /proc/cpuinfo 2>/dev/null; then
            # 3) /sys/class/dmi để biết cụ thể — có thể cần root, fallback gracefully
            local vendor
            vendor=$(cat /sys/class/dmi/id/sys_vendor 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo "")
            case "$vendor" in
                *virtualbox*) echo "VirtualBox" ;;
                *vmware*)     echo "VMware"     ;;
                *microsoft*)  echo "Hyper-V"    ;;
                *qemu*|*kvm*) echo "KVM/QEMU"  ;;
                *)            echo "VM (unknown)" ;;
            esac
            return
        fi

        echo "none"
    fi
}

# ── Smart Settings Calculator ─────────────────────────────
# Tính cấu hình tối ưu dựa trên RAM và CPU thực tế của máy.
# Sets global SMART_* variables — không in gì ra stdout.
calc_smart_settings() {
    local ram_gb=$1 cpu_cores=$2

    # Fallback nếu detect thất bại
    [[ "$ram_gb"    =~ ^[0-9]+$ ]] || ram_gb=8
    [[ "$cpu_cores" =~ ^[0-9]+$ ]] || cpu_cores=4

    # ── RAM budget (GB) ───────────────────────────────────
    # Overhead cố định: OS + Docker daemon + Redis + Prometheus + Grafana
    #                   + Zookeeper + Exporter + Debezium bookkeeping
    local overhead=3

    # Kafka broker heap: ~10% RAM, giữ trong [1, 4] GB
    local kafka_gb=$(( ram_gb / 10 ))
    [ "$kafka_gb" -lt 1 ] && kafka_gb=1
    [ "$kafka_gb" -gt 4 ] && kafka_gb=4

    # MySQL InnoDB buffer: ~12.5% RAM, giữ trong [0.5, 4] GB
    # (dưới 1GB thì dùng 512M vẫn đủ cho workload test)
    local mysql_gb=$(( ram_gb / 8 ))
    [ "$mysql_gb" -lt 1 ] && mysql_gb=0   # 0 → sẽ output "512M"
    [ "$mysql_gb" -gt 4 ] && mysql_gb=4

    # MongoDB WiredTiger cache: ~6% RAM, giữ trong [1, 4] GB
    local mongo_gb=$(( ram_gb / 16 ))
    [ "$mongo_gb" -lt 1 ] && mongo_gb=1
    [ "$mongo_gb" -gt 4 ] && mongo_gb=4

    # Debezium Connect heap (MB): ~3% RAM, giữ trong [256m, 1g]
    local debezium_mb=$(( ram_gb * 1024 / 32 ))
    [ "$debezium_mb" -lt 256  ] && debezium_mb=256
    [ "$debezium_mb" -gt 1024 ] && debezium_mb=1024

    # ── Spark: phần RAM còn lại sau khi trừ hết ──────────
    # (dùng 1GB thay 0.5GB cho mysql nếu mysql_gb=0, để budget an toàn)
    local mysql_budget=$([ "$mysql_gb" -eq 0 ] && echo 1 || echo "$mysql_gb")
    local debezium_budget=$(( debezium_mb / 1024 + 1 ))   # làm tròn lên
    local allocated=$(( overhead + kafka_gb + mysql_budget + mongo_gb + debezium_budget ))
    local spark_total=$(( ram_gb - allocated ))
    [ "$spark_total" -lt 3 ] && spark_total=3  # tối thiểu 1g/worker

    local workers=3   # giữ 3 worker (docker-compose hiện tại)
    local spark_per_worker=$(( spark_total / workers ))
    [ "$spark_per_worker" -lt 1 ] && spark_per_worker=1

    # ── CPU: trừ 2 cores cho OS, chia đều cho workers ────
    local usable_cores=$(( cpu_cores - 2 ))
    [ "$usable_cores" -lt 1 ] && usable_cores=1
    local cores_per_worker=$(( usable_cores / workers ))
    [ "$cores_per_worker" -lt 1 ] && cores_per_worker=1

    # ── Kafka partitions: scale theo cores ───────────────
    # 1 partition nếu cpu < 8 cores; 3 partition nếu đủ mạnh
    local partitions=1
    [ "$cpu_cores" -ge 8 ] && partitions=3

    # ── Set global SMART_* ────────────────────────────────
    SMART_SPARK_WORKERS=$workers
    SMART_SPARK_CORES=$cores_per_worker
    SMART_SPARK_MEM="${spark_per_worker}g"
    SMART_SPARK_TOTAL_RAM="${spark_total}g"

    if [ "$kafka_gb" -eq 1 ]; then
        SMART_KAFKA_HEAP="-Xmx1g -Xms512m"
    else
        SMART_KAFKA_HEAP="-Xmx${kafka_gb}g -Xms$(( kafka_gb / 2 ))g"
    fi

    SMART_PARTITIONS=$partitions
    SMART_MYSQL_BUFFER="$([ "$mysql_gb" -eq 0 ] && echo "512M" || echo "${mysql_gb}G")"
    SMART_MONGO_CACHE="${mongo_gb}GB"

    if [ "$debezium_mb" -lt 1024 ]; then
        SMART_DEBEZIUM_HEAP="-Xmx${debezium_mb}m -Xms$(( debezium_mb / 2 ))m"
    else
        SMART_DEBEZIUM_HEAP="-Xmx1g -Xms512m"
    fi

    # Lưu budget breakdown để hiển thị sau
    SMART_BUDGET_OVERHEAD=$overhead
    SMART_BUDGET_KAFKA=$kafka_gb
    SMART_BUDGET_MYSQL="$([ "$mysql_gb" -eq 0 ] && echo "0.5" || echo "$mysql_gb")"
    SMART_BUDGET_MONGO=$mongo_gb
    SMART_BUDGET_SPARK=$spark_total
}

# ── Profile recommendation ────────────────────────────────
recommend_profile() {
    local ram_gb=$1 battery=$2 virt=$3
    if [ "$virt" != "none" ]; then
        echo "vm"
    elif [ "$battery" = "yes" ]; then
        echo "laptop"
    elif [ "$ram_gb" -ge 32 ] 2>/dev/null; then
        echo "server"
    else
        echo "laptop"
    fi
}

recommend_reason() {
    local ram_gb=$1 battery=$2 virt=$3
    if [ "$virt" != "none" ]; then
        echo "Phát hiện môi trường ảo hóa: $virt"
    elif [ "$battery" = "yes" ]; then
        echo "Phát hiện pin — đây là laptop"
    elif [ "$ram_gb" -ge 32 ] 2>/dev/null; then
        echo "RAM >= 32GB, không có pin → workstation/server"
    else
        echo "RAM < 32GB, không xác định pin → dùng profile an toàn nhất"
    fi
}

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo -e "${BOLD}${B}╔════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${B}║     CDC Pipeline — Hardware Detector               ║${NC}"
echo -e "${BOLD}${B}╚════════════════════════════════════════════════════╝${NC}"

# ── Thu thập thông tin ────────────────────────────────────
section "Đang phát hiện..."

CPU_MODEL=$(detect_cpu_model)
CPU_CORES=$(detect_cpu_cores)
RAM_GB=$(detect_ram_gb)
BATTERY=$(detect_battery)
VIRT=$(detect_virt)
DISK_FREE=$(detect_disk_free "$PROJECT_DIR")

# Machine type label
if [ "$VIRT" != "none" ]; then
    MACHINE_TYPE="Virtual Machine ($VIRT)"
elif [ "$BATTERY" = "yes" ]; then
    MACHINE_TYPE="Laptop"
else
    MACHINE_TYPE="Desktop / Workstation"
fi

# OS label
case "$OS" in
    windows) OS_LABEL="Windows (Git Bash / MSYS2)" ;;
    linux)   OS_LABEL="Linux" ;;
    macos)   OS_LABEL="macOS" ;;
    *)       OS_LABEL="Unknown" ;;
esac

# ── Hiển thị ─────────────────────────────────────────────
section "Thông tin phần cứng"

PRIV_LABEL="$([ "$ELEVATED" = "true" ] && echo "${G}elevated (admin/root)${NC}" || echo "${Y}normal (không có quyền cao)${NC}")"

printf "  %-20s %s\n"    "OS:"             "$OS_LABEL"
printf "  %-20s "        "Quyền:";  echo -e "$PRIV_LABEL"
printf "  %-20s %s\n"    "Machine type:"   "$MACHINE_TYPE"
printf "  %-20s %s\n"    "CPU:"            "$CPU_MODEL"
printf "  %-20s %s cores (logical)\n" "CPU cores:" "$CPU_CORES"
printf "  %-20s %s GB\n" "RAM:"            "$RAM_GB"
printf "  %-20s %s\n"    "Disk free:"      "$DISK_FREE"
printf "  %-20s %s\n"    "Virtualization:" "$VIRT"
printf "  %-20s %s\n"    "Battery:"        "$([ "$BATTERY" = "yes" ] && echo "có (laptop)" || echo "không có")"

echo ""
[ "$RAM_GB" = "0" ] && warn "Không lấy được RAM — chạy lại với quyền cao để đọc đầy đủ."

# ── Tính smart settings ───────────────────────────────────
calc_smart_settings "$RAM_GB" "$CPU_CORES"

# ── Profile đề xuất ───────────────────────────────────────
RECOMMENDED=$(recommend_profile "$RAM_GB" "$BATTERY" "$VIRT")
REASON=$(recommend_reason "$RAM_GB" "$BATTERY" "$VIRT")

section "Đề xuất profile"

echo -e "  ${G}${BOLD}→ $RECOMMENDED${NC}   ($REASON)"

# ── Cấu hình tối ưu theo phần cứng thực tế ───────────────
section "Cấu hình tối ưu cho máy này"

# Hiển thị RAM budget breakdown
if [ "$RAM_GB" != "0" ]; then
    echo -e "  ${BOLD}Phân bổ RAM (${RAM_GB}GB):${NC}"
    printf "    %-28s %sGB\n" "OS + services dự trữ:"  "$SMART_BUDGET_OVERHEAD"
    printf "    %-28s %sGB\n" "Kafka broker:"           "$SMART_BUDGET_KAFKA"
    printf "    %-28s %sGB\n" "MySQL InnoDB buffer:"    "$SMART_BUDGET_MYSQL"
    printf "    %-28s %sGB\n" "MongoDB WiredTiger:"     "$SMART_BUDGET_MONGO"
    printf "    %-28s %s (%s workers × %s)\n" \
        "Spark workers:" "$SMART_SPARK_TOTAL_RAM" "$SMART_SPARK_WORKERS" "$SMART_SPARK_MEM"
    echo ""
fi

# Đọc preset của profile được đề xuất để so sánh
case "$RECOMMENDED" in
    laptop) P_WORKERS=3; P_CORES=4; P_MEM="2g"; P_KAFKA="-Xmx1g";    P_PARTS=1; P_MYSQL="512M" ;;
    server) P_WORKERS=6; P_CORES=4; P_MEM="4g"; P_KAFKA="-Xmx2g";    P_PARTS=3; P_MYSQL="2G"   ;;
    vm)     P_WORKERS=6; P_CORES=4; P_MEM="4g"; P_KAFKA="-Xmx2g";    P_PARTS=3; P_MYSQL="2G"   ;;
esac

# So sánh smart vs preset, tô màu nếu khác nhau
cmp_val() {
    local label="$1" smart="$2" preset="$3"
    if [ "$smart" = "$preset" ]; then
        printf "    %-26s ${G}%s${NC}  (= preset)\n" "$label:" "$smart"
    else
        printf "    %-26s ${G}%s${NC}  ${DIM}(preset: %s)${NC}\n" "$label:" "$smart" "$preset"
    fi
}

echo -e "  ${BOLD}Tham số đề xuất vs preset '$RECOMMENDED':${NC}"
cmp_val "Spark cores/worker"  "$SMART_SPARK_CORES"  "$P_CORES"
cmp_val "Spark mem/worker"    "$SMART_SPARK_MEM"    "$P_MEM"
cmp_val "Kafka heap"          "${SMART_KAFKA_HEAP%% *}"  "${P_KAFKA}"
cmp_val "Kafka partitions"    "$SMART_PARTITIONS"   "$P_PARTS"
cmp_val "MySQL buffer"        "$SMART_MYSQL_BUFFER" "$P_MYSQL"
echo ""

# Nếu có gì khác preset → gợi ý apply
SMART_KAFKA_MAX="${SMART_KAFKA_HEAP%% *}"   # chỉ lấy phần -XmxNg để so sánh

DIFFERS=false
[ "$SMART_SPARK_CORES" != "$P_CORES"  ] && DIFFERS=true
[ "$SMART_SPARK_MEM"   != "$P_MEM"    ] && DIFFERS=true
[ "$SMART_PARTITIONS"  != "$P_PARTS"  ] && DIFFERS=true
[ "$SMART_KAFKA_MAX"   != "$P_KAFKA"  ] && DIFFERS=true

if [ "$DIFFERS" = "true" ]; then
    echo -e "  ${Y}⚡ Có tham số khác preset. Để áp dụng, sửa pipeline/.env.$RECOMMENDED:${NC}"
    [ "$SMART_SPARK_CORES" != "$P_CORES" ] && \
        echo -e "    ${DIM}SPARK_WORKER_CORES=$SMART_SPARK_CORES${NC}"
    [ "$SMART_SPARK_MEM" != "$P_MEM" ] && \
        echo -e "    ${DIM}SPARK_WORKER_MEMORY=$SMART_SPARK_MEM${NC}"
    [ "$SMART_KAFKA_MAX" != "$P_KAFKA" ] && \
        echo -e "    ${DIM}KAFKA_HEAP_OPTS=$SMART_KAFKA_HEAP${NC}"
    [ "$SMART_PARTITIONS" != "$P_PARTS" ] && \
        echo -e "    ${DIM}KAFKA_NUM_PARTITIONS=$SMART_PARTITIONS${NC}"
    echo ""
else
    ok "Preset '$RECOMMENDED' đã phù hợp với phần cứng này."
    echo ""
fi

# ── So sánh profiles ──────────────────────────────────────
section "So sánh profiles"

printf "\n  %-26s %-12s %-12s %-12s\n" "Tham số" "laptop" "server" "vm"
printf "  %-26s %-12s %-12s %-12s\n"  \
    "──────────────────────────" "──────────" "──────────" "──────────"

rows=(
    "Spark workers|3|6|6"
    "Spark cores/worker|4|4|4"
    "Spark mem/worker|2g|4g|4g"
    "Kafka heap|-Xmx1g|-Xmx2g|-Xmx2g"
    "Kafka partitions|1|3|3"
    "MySQL InnoDB buffer|512M|2G|2G"
    "Debezium heap|-Xmx512m|-Xmx1g|-Xmx1g"
)

for row in "${rows[@]}"; do
    IFS='|' read -r param laptop server vm <<< "$row"
    # Highlight cột của recommended profile
    l_col="$laptop" s_col="$server" v_col="$vm"
    case "$RECOMMENDED" in
        laptop) l_col="${G}${laptop}${NC}" ;;
        server) s_col="${G}${server}${NC}" ;;
        vm)     v_col="${G}${vm}${NC}"     ;;
    esac
    printf "  %-26s %-22b %-22b %-22b\n" "$param" "$l_col" "$s_col" "$v_col"
done

echo ""
for p in laptop server vm; do
    if [ "$p" = "$RECOMMENDED" ]; then
        echo -e "  ${G}★ $p  ← RECOMMENDED${NC}"
    else
        echo -e "  · $p"
    fi
done

# ── Cách dùng ─────────────────────────────────────────────
section "Cách khởi động"

echo -e "  ${BOLD}Dùng profile đề xuất:${NC}"
echo -e "    bash start.sh --profile=$RECOMMENDED"
echo ""
echo -e "  ${BOLD}Chọn profile khác:${NC}"
for p in laptop server vm; do
    [ "$p" != "$RECOMMENDED" ] && echo -e "    bash start.sh --profile=$p"
done

echo ""
echo -e "  ${BOLD}Chỉnh thủ công từng tham số:${NC}"
echo -e "  Sửa trực tiếp file ${BOLD}pipeline/.env.$RECOMMENDED${NC}, ví dụ:"
echo -e "    KAFKA_NUM_PARTITIONS=3     # tăng số partition"
echo -e "    SPARK_WORKER_CORES=6       # tăng cores mỗi worker"
echo -e "    KAFKA_HEAP_OPTS=-Xmx2g -Xms1g"
echo ""
note "Override flag (--partitions=N, --kafka-heap=Xg) chưa implement trong start.sh."
note "Tracking tại TASKS.md — sẽ làm khi cần."
echo ""
