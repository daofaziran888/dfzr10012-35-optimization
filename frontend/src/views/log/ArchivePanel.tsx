import { useCallback, useState } from 'react'

import { request } from '@/api/client'

const BASE = '/api/log/archive'

type ArchiveEntry = {
  record_no: string
  row_cursor: string
  event_time: string | null
  event_offset: number | null
  time_valid: boolean
  log_content: string
  exception_type: string
  process_status: string
  archive_status: string
  archived: boolean
  processed_cursor: string | null
  resume_position: string | null
  consumed: boolean
  consumed_cursor: string | null
}

type Transfer = {
  direction: string
  entries: number
  checksum: string
  at: string
  result: string
}

type Conflict = {
  row_cursor: string
  resolved_offset: number
  indexed_cursor: string
  decision: string
  at: string
}

type ArchiveIndex = {
  archive_id: string
  state: string
  cursor_mode: string
  index_version: number
  mapping_frozen: boolean
  processed_cursor: string | null
  resume_position: string | null
  completed_files: string[]
  entries: ArchiveEntry[]
  transfers: Transfer[]
  conflicts: Conflict[]
  mapping_errors: string[]
}

type ApiResponse = {
  ok: boolean
  message: string
  index?: ArchiveIndex
  delivered?: Array<Record<string, unknown>>
  export?: ArchiveIndex & { checksum: string }
  download?: ArchiveIndex & { checksum: string }
}

const STATE_LABEL: Record<string, string> = {
  未迁移: '未迁移',
  迁移中: '迁移中',
  已中断: '已中断',
  已完成: '已完成',
}

export default function LogArchivePanel() {
  const [archiveId, setArchiveId] = useState('LOG-ARCHIVE-2026Q3')
  const [index, setIndex] = useState<ArchiveIndex | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [busy, setBusy] = useState(false)

  const invoke = useCallback(async (path: string, init?: RequestInit) => {
    setBusy(true)
    setErrorMessage('')
    try {
      const response = await request(`${BASE}/${archiveId}${path}`, init)
      const payload = (await response.json()) as ApiResponse
      if (!response.ok || payload.ok === false) {
        setErrorMessage(payload.message || '归档迁移接口调用失败')
        return payload
      }
      setMessage(payload.message)
      if (payload.index) {
        setIndex(payload.index)
      } else if (payload.export) {
        setIndex(payload.export)
      } else if (payload.download) {
        setIndex(payload.download)
      }
      return payload
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '归档迁移接口调用失败')
      return null
    } finally {
      setBusy(false)
    }
  }, [archiveId])

  const post = useCallback((path: string, body?: unknown) => invoke(path, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  }), [invoke])

  const loadIndex = useCallback(async () => {
    setBusy(true)
    setErrorMessage('')
    try {
      const response = await request(`${BASE}/${archiveId}`)
      if (response.status === 404) {
        setIndex(null)
        setMessage('该归档暂无索引，先发起一次迁移')
        return
      }
      const payload = (await response.json()) as ArchiveIndex
      setIndex(payload)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '索引读取失败')
    } finally {
      setBusy(false)
    }
  }, [archiveId])

  const exportedCount = index?.entries.filter((e) => e.archived).length ?? 0

  return (
    <section className="page archive-panel" data-module="log-archive">
      <header className="page-head">
        <div>
          <h3>日志归档游标迁移（行号 → 事件时间偏移）</h3>
          <p className="page-desc">
            同一份异常归档数据索引同时服务迁移入口与消费入口；条目保留事件偏移、已处理游标与续做位置，
            中断后续做不重读已归档区间，并发迁移只接受一个终态。
          </p>
        </div>
      </header>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void loadIndex()
        }}
      >
        <label className="filter-item">
          <span>归档编号</span>
          <input value={archiveId} onChange={(event) => setArchiveId(event.target.value)} />
        </label>
        <button className="btn" type="submit">读取索引</button>
      </form>

      <div className="archive-actions">
        <button className="btn primary" type="button" disabled={busy} onClick={() => void post('/migrate')}>
          发起迁移
        </button>
        <button
          className="btn"
          type="button"
          disabled={busy}
          onClick={() => void post('/migrate', { stop_after_row: 205 })}
          title="模拟部署中断：迁移到 row-205 后停止"
        >
          迁移并模拟中断(停 row-205)
        </button>
        <button className="btn" type="button" disabled={busy} onClick={() => void post('/resume')}>
          中断续做(从 row-206 继续)
        </button>
        <button className="btn" type="button" disabled={busy} onClick={() => void post('/consume')}>
          消费侧后续处理
        </button>
        <button className="btn ghost" type="button" disabled={busy} onClick={() => void invoke('/export')}>
          导出/上传核对
        </button>
        <button className="btn ghost" type="button" disabled={busy} onClick={() => void invoke('/download')}>
          下载核对
        </button>
      </div>

      {message && <p className="archive-message">{message}</p>}
      {errorMessage && <p className="error-text">{errorMessage}</p>}

      {index && (
        <>
          <div className="stat-row">
            <article className="stat-card">
              <span className="stat-label">迁移状态（终态唯一）</span>
              <strong className="stat-value">{STATE_LABEL[index.state] ?? index.state}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">游标模式 / 版本</span>
              <strong className="stat-value">{index.cursor_mode} / v{index.index_version}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">已处理游标</span>
              <strong className="stat-value stat-cursor">{index.processed_cursor ?? '—'}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">续做位置</span>
              <strong className="stat-value stat-cursor">{index.resume_position ?? '—'}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">已归档 / 总数</span>
              <strong className="stat-value">{exportedCount} / {index.entries.length}</strong>
            </article>
          </div>

          {index.mapping_errors.length > 0 && (
            <div className="archive-warning">
              事件时间校验异常：{index.mapping_errors.join('；')}
            </div>
          )}

          <table className="data-table">
            <thead>
              <tr>
                <th>记录编号</th>
                <th>旧行号游标</th>
                <th>事件时间</th>
                <th>事件偏移</th>
                <th>已处理游标</th>
                <th>续做位置</th>
                <th>异常类型</th>
                <th>处理状态</th>
                <th>归档状态</th>
                <th>消费状态</th>
              </tr>
            </thead>
            <tbody>
              {index.entries.map((entry) => (
                <tr key={entry.record_no}>
                  <td>{entry.record_no}</td>
                  <td>{entry.row_cursor}</td>
                  <td>{entry.time_valid ? entry.event_time : `异常：${entry.event_time || '空'}`}</td>
                  <td>{entry.event_offset ?? '—'}</td>
                  <td className="stat-cursor">{entry.processed_cursor ?? '—'}</td>
                  <td className="stat-cursor">{entry.resume_position ?? '—'}</td>
                  <td>{entry.exception_type}</td>
                  <td>{entry.process_status}</td>
                  <td>{entry.archive_status}</td>
                  <td>{entry.consumed ? `已消费(${entry.consumed_cursor ?? ''})` : '待消费'}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="archive-meta">
            <div>
              <h4>已完成文件（部署中断保留）</h4>
              <ul>
                {index.completed_files.length === 0 && <li className="empty-state">暂无</li>}
                {index.completed_files.map((file) => <li key={file}>{file}</li>)}
              </ul>
            </div>
            <div>
              <h4>上传 / 下载 / 导入核对记录</h4>
              <ul>
                {index.transfers.length === 0 && <li className="empty-state">暂无传输记录</li>}
                {index.transfers.map((transfer, i) => (
                  <li key={`${transfer.at}-${i}`}>
                    {transfer.at} [{transfer.direction}] {transfer.result}，
                    条目 {transfer.entries}，checksum {transfer.checksum.slice(0, 12)}…
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4>游标冲突裁决（只信唯一索引）</h4>
              <ul>
                {index.conflicts.length === 0 && <li className="empty-state">暂无冲突</li>}
                {index.conflicts.map((conflict, i) => (
                  <li key={`${conflict.at}-${i}`}>
                    {conflict.row_cursor} → {conflict.resolved_offset}：{conflict.decision}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </>
      )}
    </section>
  )
}
