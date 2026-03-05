-- ============================================================
-- Demo Data — CDC Pipeline
-- Chạy TRƯỚC khi bắt đầu demo để reset về trạng thái sạch
-- ============================================================

USE inventory;

-- Reset bảng
TRUNCATE TABLE customers;

-- Dữ liệu nền (3 khách hàng ban đầu — đại diện cho snapshot)
INSERT INTO customers (name, email) VALUES
  ('Nguyen Van An',   'an.nguyen@company.com'),
  ('Tran Thi Bich',   'bich.tran@company.com'),
  ('Le Hoang Cuong',  'cuong.le@company.com');

-- Kết quả mong đợi:
-- id=1  Nguyen Van An
-- id=2  Tran Thi Bich
-- id=3  Le Hoang Cuong