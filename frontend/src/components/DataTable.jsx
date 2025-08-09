import React from 'react'
import { useReactTable, getCoreRowModel, getSortedRowModel, flexRender } from '@tanstack/react-table'

export default function DataTable({columns, data}){
  const [sorting, setSorting] = React.useState([])
  const table = useReactTable({
    data, columns, state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })
  return (
    <div className="overflow-auto">
      <table className="min-w-full text-sm">
        <thead className="text-left text-slate-600 dark:text-slate-300">
          {table.getHeaderGroups().map(hg=> (
            <tr key={hg.id}>
              {hg.headers.map(h=> (
                <th key={h.id} className="px-2 py-2 cursor-pointer select-none" onClick={h.column.getToggleSortingHandler()}>
                  <div className="inline-flex items-center gap-1">
                    {flexRender(h.column.columnDef.header, h.getContext())}
                    {{asc:'↑',desc:'↓'}[h.column.getIsSorted()] || ''}
                  </div>
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
          {table.getRowModel().rows.map(r=> (
            <tr key={r.id}>
              {r.getVisibleCells().map(c=> (
                <td key={c.id} className="px-2 py-2">{flexRender(c.column.columnDef.cell, c.getContext())}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
