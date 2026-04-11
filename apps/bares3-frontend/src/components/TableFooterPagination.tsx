import { Pagination } from 'antd';

type TableFooterPaginationProps = {
  current: number;
  pageSize: number;
  total: number;
  pageSizeOptions?: number[];
  onChange: (page: number, pageSize: number) => void;
};

export function TableFooterPagination({
  current,
  pageSize,
  total,
  pageSizeOptions = [15, 50, 100, 200],
  onChange,
}: TableFooterPaginationProps) {
  if (total <= pageSize) {
    return null;
  }

  return (
    <div className="browser-table-footer">
      <Pagination
        current={current}
        onChange={onChange}
        pageSize={pageSize}
        pageSizeOptions={pageSizeOptions}
        showSizeChanger
        total={total}
      />
    </div>
  );
}
