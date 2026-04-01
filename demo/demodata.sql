USE inventory;

-- Tắt kiểm tra khóa ngoại tạm thời
SET FOREIGN_KEY_CHECKS = 0;

-- Reset sạch
TRUNCATE TABLE orders;
TRUNCATE TABLE customers;

-- Bật lại kiểm tra khóa ngoại
SET FOREIGN_KEY_CHECKS = 1;

-- Dữ liệu khách hàng
INSERT INTO customers (name, email, phone) VALUES
  ('Nguyễn Văn An', 'an.nguyen@company.com', '0912345678'),
  ('Trần Thị Bích', 'bich.tran@company.com', '0987654321'),
  ('Lê Hoàng Cường', 'cuong.le@company.com', '0978123456');

-- Dữ liệu đơn hàng (liên kết với customer_id)
INSERT INTO orders (customer_id, total_amount, status) VALUES
  (1, 1250000.00, 'DELIVERED'),  -- Đã sửa COMPLETED thành DELIVERED
  (1, 850000.00,  'PENDING'),
  (2, 2450000.00, 'SHIPPED'),
  (3, 670000.00,  'PROCESSING');