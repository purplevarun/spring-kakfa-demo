import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { Check, Database, LoaderCircle, RotateCcw, Send } from 'lucide-react'
import './orders.css'

const services = ['inventory', 'notification'] as const
type Service = (typeof services)[number]
type SavedOrder = {
  id: string
  orderId: number
  itemName: string
  processedAt: string
}
type TableState = { orders: SavedOrder[]; loaded: boolean; error: boolean }
type Receipt = {
  orderId: number
  itemName: string
  partition: number
  existed: Record<Service, boolean>
}

const timeFormat = new Intl.DateTimeFormat('en-IN', {
  timeZone: 'Asia/Kolkata',
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: true,
})

export default function Orders() {
  const [orderId, setOrderId] = useState('')
  const [itemName, setItemName] = useState('')
  const [publishing, setPublishing] = useState(false)
  const [publishError, setPublishError] = useState('')
  const [receipt, setReceipt] = useState<Receipt | null>(null)
  const [refresh, setRefresh] = useState(0)
  const [tables, setTables] = useState<Record<Service, TableState>>({
    inventory: { orders: [], loaded: false, error: false },
    notification: { orders: [], loaded: false, error: false },
  })

  useEffect(() => {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    async function loadTables() {
      await Promise.all(
        services.map(async (service) => {
          try {
            const response = await fetch(`/api/${service}/consumer`, {
              signal: AbortSignal.any([
                controller.signal,
                AbortSignal.timeout(8000),
              ]),
            })
            if (!response.ok) throw new Error('Table unavailable')
            const snapshot: { orders: SavedOrder[] } = await response.json()
            if (!controller.signal.aborted) {
              setTables((previous) => ({
                ...previous,
                [service]: {
                  orders: snapshot.orders,
                  loaded: true,
                  error: false,
                },
              }))
            }
          } catch {
            if (!controller.signal.aborted) {
              setTables((previous) => ({
                ...previous,
                [service]: { ...previous[service], error: true },
              }))
            }
          }
        }),
      )
      if (!controller.signal.aborted) timer = setTimeout(loadTables, 2000)
    }
    void loadTables()
    return () => {
      controller.abort()
      clearTimeout(timer)
    }
  }, [refresh])

  async function publish(id: number, item: string) {
    setPublishing(true)
    setPublishError('')
    try {
      const query = new URLSearchParams({ orderId: String(id), itemName: item })
      const response = await fetch(`/api/order/create?${query}`, {
        method: 'POST',
        signal: AbortSignal.timeout(15000),
      })
      if (!response.ok)
        throw new Error(`Publish failed (HTTP ${response.status}).`)
      const partitionHeader = response.headers.get('X-Kafka-Partition')
      if (
        partitionHeader === null ||
        !Number.isInteger(Number(partitionHeader))
      ) {
        throw new Error('Kafka acknowledgement is missing its partition.')
      }
      setReceipt({
        orderId: id,
        itemName: item,
        partition: Number(partitionHeader),
        existed: {
          inventory: tables.inventory.orders.some(
            (order) => order.orderId === id,
          ),
          notification: tables.notification.orders.some(
            (order) => order.orderId === id,
          ),
        },
      })
      setRefresh((previous) => previous + 1)
    } catch (error) {
      setPublishError(
        `${error instanceof Error ? error.message : 'No response from the order service.'} Check the tables before retrying; a timeout can happen after publishing.`,
      )
    } finally {
      setPublishing(false)
    }
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const id = Number(orderId)
    const item = itemName.trim()
    if (
      !orderId ||
      !Number.isInteger(id) ||
      id < -2147483648 ||
      id > 2147483647 ||
      !item
    ) {
      setPublishError('Enter an integer order ID and an item name.')
      return
    }
    void publish(id, item)
  }

  return (
    <div className="orders-app">
      <header className="site-header">
        <a href="/" className="wordmark">
          <img src="/favicon.svg" width="30" height="30" alt="" />
          Event Lab
        </a>
        <span className="environment">Local demo</span>
      </header>

      <main className="orders-main">
        <div className="page-title">
          <h1>Orders</h1>
          <span>Kafka + PostgreSQL</span>
        </div>

        <section className="publish-section" aria-labelledby="publish-heading">
          <h2 id="publish-heading">Publish an order</h2>
          <form onSubmit={submit} className="order-form">
            <label htmlFor="order-id">
              Order ID
              <input
                id="order-id"
                type="number"
                step="1"
                min="-2147483648"
                max="2147483647"
                required
                placeholder="123"
                value={orderId}
                onChange={(event) => setOrderId(event.target.value)}
              />
            </label>
            <label htmlFor="item-name">
              Item name
              <input
                id="item-name"
                required
                maxLength={160}
                placeholder="Book"
                value={itemName}
                onChange={(event) => setItemName(event.target.value)}
              />
            </label>
            <button
              className="publish-button"
              type="submit"
              disabled={publishing}
            >
              {publishing ? (
                <LoaderCircle size={17} className="spinning" />
              ) : (
                <Send size={17} />
              )}
              {publishing ? 'Publishing...' : 'Publish event'}
            </button>
          </form>
          {publishError && (
            <p className="request-error" role="alert">
              {publishError}
            </p>
          )}
        </section>

        <div
          className={`publish-result ${receipt ? 'acknowledged' : ''}`}
          role="status"
          aria-live="polite"
        >
          {receipt ? (
            <>
              <Check size={19} />
              <div>
                <strong>Event published for order #{receipt.orderId}</strong>
                <span>
                  ORDER_CREATED · Key {receipt.orderId} · Partition{' '}
                  {receipt.partition}
                </span>
              </div>
              <button
                type="button"
                className="repeat-button"
                disabled={publishing}
                onClick={() => void publish(receipt.orderId, receipt.itemName)}
              >
                <RotateCcw size={15} />
                Publish again
              </button>
            </>
          ) : (
            <>
              <Send size={17} />
              <span>No event published in this session.</span>
            </>
          )}
        </div>

        <div className="database-tables">
          {services.map((service) => {
            const table = tables[service]
            const saved = table.orders.find(
              (order) => order.orderId === receipt?.orderId,
            )
            const name = service === 'inventory' ? 'Inventory' : 'Notification'
            const result = table.error
              ? 'Unable to confirm'
              : !saved
                ? 'Waiting for persistence'
                : saved.itemName !== receipt?.itemName
                  ? 'Original item kept'
                  : receipt?.existed[service]
                    ? 'Already saved'
                    : 'Saved'
            return (
              <section
                className="database-section"
                key={service}
                aria-labelledby={`${service}-heading`}
                data-service={service}
              >
                <div className="table-heading">
                  <h2 id={`${service}-heading`}>
                    <Database size={19} />
                    {name}
                  </h2>
                  <span
                    className={`connection ${table.error ? 'offline' : ''}`}
                  >
                    <span />
                    {table.error
                      ? 'Unavailable'
                      : table.loaded
                        ? 'Connected'
                        : 'Connecting'}
                  </span>
                </div>
                <div className="table-info">
                  <code>{service}.processed_orders</code>
                  <span>
                    {table.loaded ? `${table.orders.length} rows` : '--'}
                  </span>
                </div>
                {table.error && (
                  <p className="table-error" role="alert">
                    Could not refresh this table. Any visible rows are the last
                    successful snapshot.
                  </p>
                )}
                <div className="table-scroll">
                  <table aria-label={`${name} saved orders`}>
                    <thead>
                      <tr>
                        <th scope="col">Order ID</th>
                        <th scope="col">Item name</th>
                        <th scope="col">Saved at · IST</th>
                      </tr>
                    </thead>
                    <tbody>
                      {table.orders.map((order) => (
                        <tr
                          key={order.id}
                          data-order-id={order.orderId}
                          className={
                            order.orderId === receipt?.orderId
                              ? 'highlighted'
                              : ''
                          }
                        >
                          <td className="order-number">{order.orderId}</td>
                          <td className="item-name">{order.itemName}</td>
                          <td>
                            <time dateTime={order.processedAt}>
                              {timeFormat.format(new Date(order.processedAt))}
                            </time>
                          </td>
                        </tr>
                      ))}
                      {!table.orders.length && (
                        <tr>
                          <td colSpan={3} className="empty-table">
                            {table.error
                              ? 'Table unavailable'
                              : table.loaded
                                ? 'No orders saved yet.'
                                : 'Loading saved orders...'}
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
                <div
                  className={`table-result ${saved && !table.error ? 'saved' : ''}`}
                  aria-live="polite"
                >
                  {receipt ? (
                    <>
                      <span>{result}</span>
                      <span>
                        #{receipt.orderId}
                        {saved && !table.error ? ' · 1 row' : ''}
                      </span>
                    </>
                  ) : (
                    <span>Persisted orders</span>
                  )}
                </div>
              </section>
            )
          })}
        </div>
        <footer>
          Same order ID, one saved row in each database. Repeats keep the
          original item and saved time.
        </footer>
      </main>
    </div>
  )
}
