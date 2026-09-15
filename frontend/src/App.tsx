import { useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import {
  Activity,
  ArrowDownToLine,
  ArrowRight,
  ArrowUpRight,
  Bell,
  Boxes,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleHelp,
  Clock3,
  Copy,
  Database,
  Layers3,
  Menu,
  Network,
  Package,
  Radio,
  RefreshCw,
  RotateCcw,
  Search,
  Send,
  ShieldCheck,
  ShoppingBag,
  SlidersHorizontal,
  TriangleAlert,
  Users,
  X,
} from 'lucide-react'
import {
  EmptyState,
  GroupExplorer,
  Guide,
  IconButton,
  Modal,
  OutcomeBadge,
  PartitionExplorer,
  Topology,
} from './components'
import {
  combineEvents,
  formatTime,
  requestJson,
  serviceLabels,
  services,
  totalLag,
  useDashboard,
} from './data'
import type { EventRow, PublishedOrder, Service } from './data'
import './App.css'

type View = 'flow' | 'partitions' | 'groups' | 'history'
const navigation = [
  { id: 'flow', label: 'Event flow', icon: Network },
  { id: 'partitions', label: 'Partitions', icon: Layers3 },
  { id: 'groups', label: 'Consumer groups', icon: Users },
  { id: 'history', label: 'Delivery history', icon: Database },
] as const
const descriptions = {
  flow: 'One event. Two independent consumers. Follow the whole journey.',
  partitions: 'See where records live and how far each consumer has read.',
  groups: 'Separate subscriptions. Independent progress. The same events.',
  history: 'An audit trail of deliveries and the orders saved in PostgreSQL.',
}

function Metric({
  label,
  value,
  detail,
  children,
  tone,
}: {
  label: string
  value: ReactNode
  detail: ReactNode
  children: ReactNode
  tone: string
}) {
  return (
    <div className={`metric ${tone}`}>
      <div className="metric-label">
        {label}
        {children}
      </div>
      <strong>{value}</strong>
      <span className="metric-detail">{detail}</span>
    </div>
  )
}

function App() {
  const [view, setView] = useState<View>('flow')
  const [mobileMenu, setMobileMenu] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)
  const data = useDashboard(refreshKey)
  const [published, setPublished] = useState<PublishedOrder[]>([])
  const [orderId, setOrderId] = useState('1001')
  const [itemName, setItemName] = useState('Studio headphones')
  const [publishing, setPublishing] = useState(false)
  const [busyConsumer, setBusyConsumer] = useState<Service | null>(null)
  const [notice, setNotice] = useState<{ error: boolean; text: string } | null>(
    null,
  )
  const [guideOpen, setGuideOpen] = useState(false)
  const [selected, setSelected] = useState<EventRow | null>(null)
  const [search, setSearch] = useState('')
  const [outcomeFilter, setOutcomeFilter] = useState('all')
  const [partitionFilter, setPartitionFilter] = useState('all')
  const [serviceFilter, setServiceFilter] = useState('all')
  const [tableMode, setTableMode] = useState<'deliveries' | 'orders'>(
    'deliveries',
  )
  const [page, setPage] = useState(0)
  const events = combineEvents(data, published)
  const selectedEvent = selected
    ? (events.find((event) => event.id === selected.id) ?? selected)
    : null
  const latest = published[0]
  const errors = Object.entries(data.errors)
  const totalProcessed =
    (data.inventory?.counts.processed ?? 0) +
    (data.notification?.counts.processed ?? 0)
  const totalDuplicates =
    (data.inventory?.counts.duplicates ?? 0) +
    (data.notification?.counts.duplicates ?? 0)
  const retainedRecords = data.kafka?.partitions.reduce(
    (sum, partition) => sum + partition.endOffset - partition.startOffset,
    0,
  )
  const lag = data.kafka?.groups.reduce(
    (sum, group) => sum + totalLag(group),
    0,
  )
  const title = navigation.find((item) => item.id === view)!.label
  const query = search.toLowerCase()
  const filteredEvents = events.filter((event) => {
    if (!`${event.orderId} ${event.itemName}`.toLowerCase().includes(query))
      return false
    if (
      partitionFilter !== 'all' &&
      event.partition !== Number(partitionFilter)
    )
      return false
    if (
      serviceFilter !== 'all' &&
      outcomeFilter !== 'pending' &&
      !event[serviceFilter as Service]
    )
      return false
    const relevantServices =
      serviceFilter === 'all' ? services : [serviceFilter as Service]
    return (
      outcomeFilter === 'all' ||
      relevantServices.some((service) =>
        outcomeFilter === 'pending'
          ? !event[service]
          : event[service]?.outcome === outcomeFilter,
      )
    )
  })
  const storedOrders = services
    .flatMap((service) =>
      (data[service]?.orders ?? []).map((order) => ({ ...order, service })),
    )
    .filter(
      (order) =>
        (serviceFilter === 'all' || serviceFilter === order.service) &&
        `${order.orderId} ${order.itemName}`.toLowerCase().includes(query),
    )
    .sort(
      (first, second) =>
        Date.parse(second.processedAt) - Date.parse(first.processedAt),
    )
  const resultCount =
    tableMode === 'deliveries' ? filteredEvents.length : storedOrders.length
  const pageCount = Math.max(1, Math.ceil(resultCount / 10))
  const currentPage = Math.min(page, pageCount - 1)

  function navigate(next: View) {
    setView(next)
    setMobileMenu(false)
    setPage(0)
  }

  function inspectPartition(partition: number) {
    setPartitionFilter(String(partition))
    setTableMode('deliveries')
    setOutcomeFilter('all')
    navigate('history')
  }

  async function publishOrder(id: number, item: string, advance = false) {
    setPublishing(true)
    setNotice(null)
    try {
      const query = new URLSearchParams({ orderId: String(id), itemName: item })
      const response = await fetch(`/api/order/create?${query}`, {
        method: 'POST',
        signal: AbortSignal.timeout(15000),
      })
      if (!response.ok)
        throw new Error(`The order API returned HTTP ${response.status}.`)
      const partition = Number(response.headers.get('X-Kafka-Partition'))
      const offset = Number(response.headers.get('X-Kafka-Offset'))
      if (
        !response.headers.has('X-Kafka-Partition') ||
        !response.headers.has('X-Kafka-Offset')
      ) {
        throw new Error('The API response is missing Kafka record coordinates.')
      }
      const receipt = {
        orderId: id,
        itemName: item,
        partition,
        offset,
        timestamp: new Date().toISOString(),
      }
      setPublished((previous) => [receipt, ...previous].slice(0, 80))
      if (advance && id < 2147483647) setOrderId(String(id + 1))
      setNotice({
        error: false,
        text: `Order #${id} acknowledged by Kafka. Partition ${partition}, offset ${offset}.`,
      })
      setRefreshKey((previous) => previous + 1)
    } catch (error) {
      setNotice({
        error: true,
        text: `${error instanceof Error ? error.message : 'Publish failed.'} Check delivery history before retrying; a timeout does not prove the write failed.`,
      })
    } finally {
      setPublishing(false)
    }
  }

  function submitOrder(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const id = Number(orderId)
    if (
      !Number.isInteger(id) ||
      id < -2147483648 ||
      id > 2147483647 ||
      !orderId ||
      !itemName.trim()
    ) {
      setNotice({
        error: true,
        text: 'Enter a valid 32-bit integer order ID and a non-empty item name.',
      })
      return
    }
    void publishOrder(id, itemName.trim(), true)
  }

  async function toggleConsumer(service: Service) {
    if (!data[service]) return
    setBusyConsumer(service)
    const paused = !data[service].pauseRequested
    try {
      await requestJson(`/api/${service}/consumer/pause?paused=${paused}`, {
        method: 'POST',
      })
      setNotice({
        error: false,
        text: `${serviceLabels[service]} ${paused ? 'pause' : 'resume'} requested. The other group is unaffected.`,
      })
      setRefreshKey((previous) => previous + 1)
    } catch {
      setNotice({
        error: true,
        text: `Could not change the ${service} consumer. Check that its API is available.`,
      })
    } finally {
      setBusyConsumer(null)
    }
  }

  function exportCsv() {
    const rows =
      tableMode === 'deliveries'
        ? [
            [
              'orderId',
              'itemName',
              'partition',
              'offset',
              'inventory',
              'notification',
              'observedAt',
            ],
            ...filteredEvents.map((event) => [
              event.orderId,
              event.itemName,
              event.partition,
              event.offset,
              event.inventory?.outcome ?? 'PENDING',
              event.notification?.outcome ?? 'PENDING',
              event.timestamp,
            ]),
          ]
        : [
            ['id', 'orderId', 'itemName', 'service', 'processedAt'],
            ...storedOrders.map((order) => [
              order.id,
              order.orderId,
              order.itemName,
              order.service,
              order.processedAt,
            ]),
          ]
    const csv = rows
      .map((row) =>
        row
          .map((value) => {
            const cell = String(value)
            const safe = /^[=+@\-\t\r]/.test(cell) ? `'${cell}` : cell
            return `"${safe.replaceAll('"', '""')}"`
          })
          .join(','),
      )
      .join('\r\n')
    const url = URL.createObjectURL(
      new Blob([csv], { type: 'text/csv;charset=utf-8' }),
    )
    const link = document.createElement('a')
    link.href = url
    link.download = `event-lab-${tableMode}.csv`
    link.click()
    URL.revokeObjectURL(url)
  }

  async function copyPayload(event: EventRow) {
    try {
      await navigator.clipboard.writeText(
        JSON.stringify(
          {
            eventType: 'ORDER_CREATED',
            payload: { orderId: event.orderId, itemName: event.itemName },
          },
          null,
          2,
        ),
      )
      setNotice({ error: false, text: 'Event JSON copied.' })
    } catch {
      setNotice({
        error: true,
        text: 'Clipboard access is unavailable in this browser.',
      })
    }
  }

  const history = (
    <section className="history-section">
      <div className="section-heading">
        <h2>
          <Activity size={18} />
          {view === 'flow' ? 'Event activity' : 'Processing ledger'}
        </h2>
        <div className="section-actions">
          <span className="quiet-label">{resultCount} records</span>
          <IconButton
            label="Export filtered records as CSV"
            disabled={!resultCount}
            onClick={exportCsv}
          >
            <ArrowDownToLine size={16} />
          </IconButton>
        </div>
      </div>
      <div className="history-toolbar">
        <div className="segmented" aria-label="History view">
          {(['deliveries', 'orders'] as const).map((mode) => (
            <button
              key={mode}
              className={tableMode === mode ? 'selected' : ''}
              aria-pressed={tableMode === mode}
              onClick={() => {
                setTableMode(mode)
                setPage(0)
              }}
            >
              {mode === 'deliveries' ? 'Kafka deliveries' : 'Saved orders'}
            </button>
          ))}
        </div>
        <label className="search-field">
          <Search size={15} />
          <input
            aria-label="Search order ID or item"
            type="search"
            placeholder="Search orders or items"
            value={search}
            onChange={(event) => {
              setSearch(event.target.value)
              setPage(0)
            }}
          />
        </label>
      </div>
      <div className="filter-row">
        <SlidersHorizontal size={14} />
        <select
          aria-label="Filter consumer"
          value={serviceFilter}
          onChange={(event) => {
            setServiceFilter(event.target.value)
            setPage(0)
          }}
        >
          <option value="all">Both consumers</option>
          <option value="inventory">Inventory</option>
          <option value="notification">Notification</option>
        </select>
        {tableMode === 'deliveries' && (
          <>
            <select
              aria-label="Filter outcome"
              value={outcomeFilter}
              onChange={(event) => {
                setOutcomeFilter(event.target.value)
                setPage(0)
              }}
            >
              <option value="all">All outcomes</option>
              <option value="PROCESSED">Processed</option>
              <option value="DUPLICATE">Duplicate skipped</option>
              <option value="CONFLICT">Conflict blocked</option>
              <option value="pending">Awaiting consumer</option>
            </select>
            <select
              aria-label="Filter partition"
              value={partitionFilter}
              onChange={(event) => {
                setPartitionFilter(event.target.value)
                setPage(0)
              }}
            >
              <option value="all">All partitions</option>
              {data.kafka?.partitions.map((partition) => (
                <option key={partition.id} value={partition.id}>
                  Partition {partition.id}
                </option>
              ))}
            </select>
          </>
        )}
        {(search ||
          partitionFilter !== 'all' ||
          outcomeFilter !== 'all' ||
          serviceFilter !== 'all') && (
          <button
            className="text-button reset-filter"
            onClick={() => {
              setSearch('')
              setPartitionFilter('all')
              setOutcomeFilter('all')
              setServiceFilter('all')
              setPage(0)
            }}
          >
            Reset <X size={12} />
          </button>
        )}
      </div>
      {resultCount === 0 ? (
        <EmptyState
          title={
            events.length
              ? 'No matching records'
              : data.updatedAt
                ? 'Ready for the next event'
                : 'Connecting to your services'
          }
          text={
            events.length
              ? 'Adjust the search or filters to see more records.'
              : 'Publish an order to see its path through both consumers.'
          }
        />
      ) : (
        <div className="table-scroll">
          <table className="event-table">
            <thead>
              {tableMode === 'deliveries' ? (
                <tr>
                  <th>Order / item</th>
                  <th>Kafka record</th>
                  <th>Inventory</th>
                  <th>Notification</th>
                  <th>Observed · IST</th>
                  <th />
                </tr>
              ) : (
                <tr>
                  <th>Order / item</th>
                  <th>Database</th>
                  <th>Processing ID</th>
                  <th>Saved · IST</th>
                  <th />
                </tr>
              )}
            </thead>
            <tbody>
              {tableMode === 'deliveries'
                ? filteredEvents
                    .slice(currentPage * 10, currentPage * 10 + 10)
                    .map((event) => (
                      <tr
                        key={event.id}
                        data-order-id={event.orderId}
                        className={
                          latest?.partition === event.partition &&
                          latest?.offset === event.offset
                            ? 'latest-row'
                            : ''
                        }
                      >
                        <td>
                          <button
                            className="order-link"
                            onClick={() => setSelected(event)}
                          >
                            #{event.orderId}
                            <ArrowUpRight size={12} />
                          </button>
                          <span className="item-cell" title={event.itemName}>
                            {event.itemName}
                          </span>
                        </td>
                        <td>
                          <span
                            className={`partition-tag partition-color-${event.partition % 3}`}
                          >
                            P{event.partition}
                          </span>
                          <code className="offset-value">#{event.offset}</code>
                        </td>
                        <td>
                          <OutcomeBadge outcome={event.inventory?.outcome} />
                        </td>
                        <td>
                          <OutcomeBadge outcome={event.notification?.outcome} />
                        </td>
                        <td className="time-cell">
                          {formatTime(event.timestamp)}
                        </td>
                        <td>
                          <IconButton
                            label={`Inspect order ${event.orderId}, offset ${event.offset}`}
                            onClick={() => setSelected(event)}
                          >
                            <ChevronRight size={15} />
                          </IconButton>
                        </td>
                      </tr>
                    ))
                : storedOrders
                    .slice(currentPage * 10, currentPage * 10 + 10)
                    .map((order) => (
                      <tr key={order.id}>
                        <td>
                          <strong className="stored-order-id">
                            #{order.orderId}
                          </strong>
                          <span className="item-cell" title={order.itemName}>
                            {order.itemName}
                          </span>
                        </td>
                        <td>
                          <span className={`service-label ${order.service}`}>
                            <Database size={13} />
                            {serviceLabels[order.service]}
                          </span>
                        </td>
                        <td>
                          <code className="short-id" title={order.id}>
                            {order.id.slice(0, 8)}...
                          </code>
                        </td>
                        <td className="time-cell">
                          {formatTime(order.processedAt, true)}
                        </td>
                        <td>
                          <IconButton
                            label={`Replay order ${order.orderId}`}
                            disabled={publishing}
                            onClick={() =>
                              void publishOrder(order.orderId, order.itemName)
                            }
                          >
                            <RotateCcw size={15} />
                          </IconButton>
                        </td>
                      </tr>
                    ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="table-footer">
        <span>Recent 200 records per consumer · persisted in PostgreSQL</span>
        <div>
          <IconButton
            label="Previous page"
            disabled={currentPage === 0}
            onClick={() => setPage(currentPage - 1)}
          >
            <ChevronLeft size={15} />
          </IconButton>
          <span>
            {currentPage + 1} / {pageCount}
          </span>
          <IconButton
            label="Next page"
            disabled={currentPage >= pageCount - 1}
            onClick={() => setPage(currentPage + 1)}
          >
            <ChevronRight size={15} />
          </IconButton>
        </div>
      </div>
    </section>
  )

  return (
    <div className="app-shell">
      {mobileMenu && (
        <button
          className="sidebar-backdrop"
          aria-label="Close navigation"
          onClick={() => setMobileMenu(false)}
        />
      )}
      <aside className={`sidebar ${mobileMenu ? 'open' : ''}`}>
        <a
          className="brand"
          href="#"
          onClick={(event) => {
            event.preventDefault()
            navigate('flow')
          }}
        >
          <span className="brand-mark">
            <Network size={23} />
          </span>
          <span>
            Event Lab<small>SPRING + KAFKA</small>
          </span>
        </a>
        <div className="workspace-switch">
          <span className="workspace-icon">
            <Boxes size={17} />
          </span>
          <div>
            Kafka demo<small>Local workspace</small>
          </div>
          <span className="environment-dot" />
        </div>
        <span className="nav-label">WORKSPACE</span>
        <nav aria-label="Main navigation">
          {navigation.map((item) => (
            <button
              key={item.id}
              className={view === item.id ? 'active' : ''}
              aria-current={view === item.id ? 'page' : undefined}
              onClick={() => navigate(item.id)}
            >
              <item.icon size={18} />
              {item.label}
              {item.id === 'groups' && <span className="nav-count">2</span>}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <span className="nav-label">CONNECTIONS</span>
          {(
            [
              { name: 'Kafka broker', source: 'kafka', icon: Radio },
              {
                name: 'Inventory database',
                source: 'inventory',
                icon: Database,
              },
              {
                name: 'Notification database',
                source: 'notification',
                icon: Database,
              },
            ] as const
          ).map((item) => (
            <div className="connection" key={item.source}>
              <item.icon size={14} />
              <span>{item.name}</span>
              <span
                title={
                  data.errors[item.source] ??
                  (data[item.source] ? 'Connected' : 'Connecting')
                }
                className={`status-dot ${data.errors[item.source] ? 'bad' : data[item.source] ? 'green' : ''}`}
              />
            </div>
          ))}
          <button className="learn-button" onClick={() => setGuideOpen(true)}>
            <CircleHelp size={17} />
            Understand the flow
            <ArrowUpRight size={14} />
          </button>
          <div className="sidebar-version">
            Local development<span>v1.0</span>
          </div>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            <IconButton
              label="Open navigation"
              className="mobile-menu-button"
              onClick={() => setMobileMenu(true)}
            >
              <Menu size={18} />
            </IconButton>
            <Boxes size={16} />
            <span>Kafka demo</span>
            <ChevronRight size={13} />
            <strong>{title}</strong>
          </div>
          <div className="topbar-actions">
            <span className={`live-pill ${errors.length ? 'degraded' : ''}`}>
              <span className="status-dot" />
              {errors.length
                ? 'Degraded'
                : data.updatedAt
                  ? 'Live'
                  : 'Connecting'}
            </span>
            <span className="sync-label">2s sync</span>
            <IconButton
              label="Refresh live data"
              onClick={() => setRefreshKey((previous) => previous + 1)}
            >
              <RefreshCw size={16} />
            </IconButton>
            <span className="topbar-divider" />
            <IconButton
              label="Open event flow guide"
              onClick={() => setGuideOpen(true)}
            >
              <CircleHelp size={18} />
            </IconButton>
          </div>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <div className="eyebrow">
                <span className="green-line" />
                EVENT-DRIVEN WORKSPACE
              </div>
              <h1>{title}</h1>
              <p>{descriptions[view]}</p>
            </div>
            {view === 'flow' ? (
              <div className="last-sync">
                <Clock3 size={14} />
                <span>
                  {formatTime(data.updatedAt)} IST
                  <br />
                  <small>Last snapshot</small>
                </span>
              </div>
            ) : (
              <button
                className="primary-button small"
                onClick={() => navigate('flow')}
              >
                <Send size={15} />
                Publish an order
              </button>
            )}
          </div>
          {errors.length > 0 && (
            <div className="error-banner" role="alert">
              <TriangleAlert size={18} />
              <div>
                <strong>Some live data is unavailable</strong>
                <span>
                  {errors
                    .map(([source, error]) => `${source}: ${error}`)
                    .join(' · ')}
                  . Any retained values are the last successful snapshot.
                </span>
              </div>
            </div>
          )}
          <div className="metrics-band">
            <Metric
              label="Records in Kafka"
              tone="blue"
              value={retainedRecords ?? '--'}
              detail={`${data.kafka?.partitions.length ?? '--'} partitions in order-events`}
            >
              <Layers3 size={17} />
            </Metric>
            <Metric
              label="Orders processed"
              tone="green"
              value={
                data.inventory && data.notification ? totalProcessed : '--'
              }
              detail={`${data.inventory?.counts.processed ?? '--'} inventory · ${data.notification?.counts.processed ?? '--'} notification`}
            >
              <CheckCircle2 size={17} />
            </Metric>
            <Metric
              label="Duplicates skipped"
              tone="amber"
              value={
                data.inventory && data.notification ? totalDuplicates : '--'
              }
              detail="Across both consumer databases"
            >
              <ShieldCheck size={17} />
            </Metric>
            <Metric
              label="Consumer lag"
              tone="coral"
              value={lag ?? '--'}
              detail={
                lag === 0
                  ? 'Both groups are caught up'
                  : 'Records awaiting consumption'
              }
            >
              <Activity size={17} />
            </Metric>
          </div>
          {view === 'flow' && (
            <div className="workspace-grid">
              <div className="flow-main">
                <Topology
                  data={data}
                  publication={
                    latest ? `${latest.partition}:${latest.offset}` : ''
                  }
                  busy={busyConsumer}
                  onPause={(service) => void toggleConsumer(service)}
                  onPartition={inspectPartition}
                />
                {history}
              </div>
              <aside className="composer-column">
                <section className="composer">
                  <div className="composer-heading">
                    <span className="composer-icon">
                      <Send size={18} />
                    </span>
                    <div>
                      <h2>Publish an order</h2>
                      <span>Start a journey through Kafka</span>
                    </div>
                  </div>
                  <form onSubmit={submitOrder}>
                    <label htmlFor="order-id">
                      Order ID<span>INTEGER · KAFKA KEY</span>
                    </label>
                    <div className="input-with-symbol">
                      <span>#</span>
                      <input
                        id="order-id"
                        type="number"
                        required
                        step="1"
                        min="-2147483648"
                        max="2147483647"
                        value={orderId}
                        onChange={(event) => setOrderId(event.target.value)}
                      />
                    </div>
                    <label htmlFor="item-name">
                      Item name<span>STRING</span>
                    </label>
                    <input
                      id="item-name"
                      required
                      maxLength={160}
                      value={itemName}
                      onChange={(event) => setItemName(event.target.value)}
                      placeholder="e.g. Studio headphones"
                    />
                    <div className="event-preview-heading">
                      <span className="eyebrow">EVENT PAYLOAD</span>
                      <span className="tiny-tag">JSON</span>
                    </div>
                    <pre className="event-preview">
                      <span className="json-brace">{'{'}</span>
                      {'\n  '}
                      <span className="json-key">"eventType"</span>
                      {': '}
                      <span className="json-string">"ORDER_CREATED"</span>
                      {',\n  '}
                      <span className="json-key">"payload"</span>
                      {': {\n    '}
                      <span className="json-key">"orderId"</span>
                      {': '}
                      <span className="json-number">{orderId || 'null'}</span>
                      {',\n    '}
                      <span className="json-key">"itemName"</span>
                      {': '}
                      <span className="json-string">
                        {JSON.stringify(itemName)}
                      </span>
                      {'\n  }\n}'}
                    </pre>
                    <button
                      className="primary-button publish-button"
                      type="submit"
                      disabled={
                        publishing || !!data.errors.kafka || !data.kafka
                      }
                    >
                      {publishing ? (
                        <RefreshCw size={16} className="spin" />
                      ) : (
                        <Send size={16} />
                      )}
                      {publishing ? 'Publishing...' : 'Publish event'}
                      <ArrowRight size={16} />
                    </button>
                  </form>
                  <button
                    className="replay-button"
                    disabled={!latest || publishing}
                    onClick={() =>
                      latest &&
                      void publishOrder(latest.orderId, latest.itemName)
                    }
                  >
                    <RotateCcw size={14} />
                    Replay last order{latest && <code>#{latest.orderId}</code>}
                  </button>
                  {latest && (
                    <div className="publish-receipt">
                      <CheckCircle2 size={15} />
                      <div>
                        <strong>Kafka acknowledged #{latest.orderId}</strong>
                        <span>
                          Partition {latest.partition} · offset {latest.offset}
                        </span>
                      </div>
                    </div>
                  )}
                </section>
                <section className="idempotency-note">
                  <span className="note-icon">
                    <ShieldCheck size={20} />
                  </span>
                  <h3>Same event. One effect.</h3>
                  <p>
                    Replay an order to see idempotency in action. Each consumer
                    saves the first order and skips identical repeats.
                  </p>
                  <div className="unique-key">
                    <Database size={13} />
                    <code>UNIQUE(orderId, eventType)</code>
                  </div>
                  <button
                    className="text-button"
                    onClick={() => setGuideOpen(true)}
                  >
                    What happens on a replay?
                    <ArrowUpRight size={13} />
                  </button>
                </section>
              </aside>
            </div>
          )}
          {view === 'partitions' && (
            <PartitionExplorer data={data} onHistory={inspectPartition} />
          )}
          {view === 'groups' && (
            <GroupExplorer
              data={data}
              busy={busyConsumer}
              onPause={(service) => void toggleConsumer(service)}
            />
          )}
          {view === 'history' && history}
          <footer className="page-footer">
            <span>
              <Radio size={12} />
              {data.kafka?.topic ?? 'order-events'}
              <span className="footer-dot">/</span>Apache Kafka
            </span>
            <span>
              <ShieldCheck size={12} />
              Independent PostgreSQL stores<span className="footer-dot">/</span>
              Times in IST
            </span>
          </footer>
        </main>
      </div>
      {notice && (
        <div
          className={`toast ${notice.error ? 'error' : ''}`}
          role={notice.error ? 'alert' : 'status'}
        >
          {notice.error ? (
            <TriangleAlert size={19} />
          ) : (
            <CheckCircle2 size={19} />
          )}
          <p>{notice.text}</p>
          <IconButton label="Dismiss message" onClick={() => setNotice(null)}>
            <X size={15} />
          </IconButton>
        </div>
      )}
      {guideOpen && <Guide onClose={() => setGuideOpen(false)} />}
      {selectedEvent && (
        <Modal
          title={`Order #${selectedEvent.orderId}`}
          onClose={() => setSelected(null)}
        >
          <div className="record-summary">
            <span className="record-item-icon">
              <ShoppingBag size={24} />
            </span>
            <div>
              <h3>{selectedEvent.itemName}</h3>
              <p>ORDER_CREATED</p>
            </div>
          </div>
          <dl className="record-metadata">
            <div>
              <dt>Topic</dt>
              <dd>order-events</dd>
            </div>
            <div>
              <dt>Kafka key</dt>
              <dd>
                {selectedEvent.orderId} <small>integer</small>
              </dd>
            </div>
            <div>
              <dt>Partition</dt>
              <dd>P{selectedEvent.partition}</dd>
            </div>
            <div>
              <dt>Offset</dt>
              <dd>{selectedEvent.offset}</dd>
            </div>
          </dl>
          <div className="section-heading">
            <h3>Event payload</h3>
            <IconButton
              label="Copy event JSON"
              onClick={() => void copyPayload(selectedEvent)}
            >
              <Copy size={16} />
            </IconButton>
          </div>
          <pre className="detail-json">
            {JSON.stringify(
              {
                eventType: 'ORDER_CREATED',
                payload: {
                  orderId: selectedEvent.orderId,
                  itemName: selectedEvent.itemName,
                },
              },
              null,
              2,
            )}
          </pre>
          <h3 className="detail-subtitle">Consumer receipts</h3>
          {services.map((service) => (
            <div className={`receipt-status ${service}`} key={service}>
              <div>
                <span className="service-label">
                  {service === 'inventory' ? (
                    <Package size={16} />
                  ) : (
                    <Bell size={16} />
                  )}
                  {serviceLabels[service]}
                </span>
                <OutcomeBadge outcome={selectedEvent[service]?.outcome} />
              </div>
              <p>
                {selectedEvent[service]
                  ? `${formatTime(selectedEvent[service].receivedAt, true)} IST`
                  : 'This consumer has not recorded this event yet.'}
              </p>
              {selectedEvent[service] && (
                <code>Receipt {selectedEvent[service].id}</code>
              )}
              {selectedEvent[service]?.outcome === 'CONFLICT' && (
                <p className="warning-text">
                  This order ID already has a different item. The original
                  database record was not changed.
                </p>
              )}
            </div>
          ))}
          <div className="modal-footer">
            <p>
              A replay publishes a new Kafka record with the same order key.
            </p>
            <button
              className="primary-button"
              disabled={publishing}
              onClick={() =>
                void publishOrder(selectedEvent.orderId, selectedEvent.itemName)
              }
            >
              <RotateCcw size={15} />
              Replay this order
            </button>
          </div>
        </Modal>
      )}
    </div>
  )
}

export default App
