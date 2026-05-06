#!/bin/bash
# ============================================================
# detect_hardware.sh — Phát hiện phần cứng, đề xuất profile
#
# Hỗ trợ: Windows (Git Bash / MSYS2) + Linux
# Không crash nếu thiếu quyền admin/root — fallback gracefully
#
# Usage:
#   bash detect_hardware.sh
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

printf "  %-20s %s\n" "OS:"             "$OS_LABEL"
printf "  %-20s %s\n" "Machine type:"   "$MACHINE_TYPE"
printf "  %-20s %s\n" "CPU:"            "$CPU_MODEL"
printf "  %-20s %s cores (logical)\n" "CPU cores:"    "$CPU_CORES"
printf "  %-20s %s GB\n" "RAM:"          "$RAM_GB"
printf "  %-20s %s\n" "Disk free:"      "$DISK_FREE"
printf "  %-20s %s\n" "Virtualization:" "$VIRT"
printf "  %-20s %s\n" "Battery:"        "$([ "$BATTERY" = "yes" ] && echo "có (laptop)" || echo "không có")"

# Cảnh báo nếu thiếu quyền
echo ""
if [ "$RAM_GB" = "0" ]; then
    warn "Không lấy được RAM. Thử chạy terminal với quyền Admin (Windows) hoặc sudo (Linux)."
fi
if [ "$VIRT" = "none" ] && [ "$OS" = "linux" ]; then
    note "VM detection đầy đủ hơn nếu chạy: sudo bash detect_hardware.sh"
fi
if [ "$OS" = "windows" ]; then
    note "Nếu thông tin thiếu: click phải Git Bash → 'Run as Administrator' rồi chạy lại."
fi

# ── Profile đề xuất ───────────────────────────────────────
RECOMMENDED=$(recommend_profile "$RAM_GB" "$BATTERY" "$VIRT")
REASON=$(recommend_reason "$RAM_GB" "$BATTERY" "$VIRT")

section "Đề xuất profile"

echo -e "  ${G}${BOLD}→ $RECOMMENDED${NC}   ($REASON)"

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
