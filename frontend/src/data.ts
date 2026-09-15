import { useEffect, useState } from 'react'

export type Service = 'inventory' | 'notification'
export type Outcome = 'PROCESSED' | 'DUPLICATE' | 'CONFLICT'
export type Delivery = {
  id: string
  orderId: number
  itemName: string
  eventType: string
  partition: number
  offset: number
  outcome: Outcome
  receivedAt: string
}
export type StoredOrder = {
  id: string
  orderId: number
  itemName: string
  eventType: string
  processedAt: string
}
export type ConsumerSnapshot = {
  service: Service
  groupId: string
  pauseRequested: boolean
  paused: boolean
  running: boolean
  counts: {
    deliveries: number
    processed: number
    duplicates: number
    conflicts: number
  }
  orders: StoredOrder[]
  deliveries: Delivery[]
}
export type Partition = {
  id: number
  leader: number
  replicas: number[]
  inSyncReplicas: number[]
  startOffset: number
  endOffset: number
}
export type Group = {
  groupId: string
  service: Service
  state: string
  members: {
    id: string
    clientId: string
    host: string
    partitions: number[]
  }[]
  partitions: {
    id: number
    committedOffset: number | null
    endOffset: number
    lag: number
  }[]
}
export type KafkaSnapshot = {
  topic: string
  clusterId: string
  sampledAt: string
  brokers: { id: number; host: string; port: number }[]
  partitions: Partition[]
  groups: Group[]
}
export type PublishedOrder = {
  orderId: number
  itemName: string
  partition: number
  offset: number
  timestamp: string
}
export type EventRow = PublishedOrder & {
  id: string
  inventory?: Delivery
  notification?: Delivery
}
export type DashboardData = {
  kafka: KafkaSnapshot | null
  inventory: ConsumerSnapshot | null
  notification: ConsumerSnapshot | null
  errors: Partial<Record<'kafka' | Service, string>>
  updatedAt: string | null
}

export const services: Service[] = ['inventory', 'notification']
export const serviceLabels = {
  inventory: 'Inventory',
  notification: 'Notification',
}

export async function requestJson<Type>(
  path: string,
  init?: RequestInit,
): Promise<Type> {
  const response = await fetch(path, {
    signal: AbortSignal.timeout(10000),
    ...init,
  })
  if (!response.ok) throw new Error(`HTTP ${response.status}`)
  return response.json() as Promise<Type>
}

export function useDashboard(refreshKey: number) {
  const [data, setData] = useState<DashboardData>({
    kafka: null,
    inventory: null,
    notification: null,
    errors: {},
    updatedAt: null,
  })
  useEffect(() => {
    let disposed = false
    let timer: ReturnType<typeof setTimeout>
    async function refresh() {
      const results = await Promise.allSettled([
        requestJson<KafkaSnapshot>('/api/order/kafka'),
        requestJson<ConsumerSnapshot>('/api/inventory/consumer'),
        requestJson<ConsumerSnapshot>('/api/notification/consumer'),
      ])
      if (disposed) return
      const errors: DashboardData['errors'] = {}
      const sources = ['kafka', 'inventory', 'notification'] as const
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          errors[sources[index]] =
            result.reason instanceof Error
              ? result.reason.message
              : 'Unavailable'
        }
      })
      setData((previous) => ({
        kafka:
          results[0].status === 'fulfilled' ? results[0].value : previous.kafka,
        inventory:
          results[1].status === 'fulfilled'
            ? results[1].value
            : previous.inventory,
        notification:
          results[2].status === 'fulfilled'
            ? results[2].value
            : previous.notification,
        errors,
        updatedAt: new Date().toISOString(),
      }))
      timer = setTimeout(refresh, 2000)
    }
    void refresh()
    return () => {
      disposed = true
      clearTimeout(timer)
    }
  }, [refreshKey])
  return data
}

export function combineEvents(
  data: DashboardData,
  published: PublishedOrder[],
): EventRow[] {
  const records = new Map<string, EventRow>()
  for (const service of services) {
    for (const delivery of data[service]?.deliveries ?? []) {
      const id = `${delivery.partition}:${delivery.offset}`
      const existing = records.get(id)
      records.set(id, {
        ...delivery,
        id,
        timestamp: delivery.receivedAt,
        ...existing,
        [service]: delivery,
      })
    }
  }
  for (const receipt of published) {
    const id = `${receipt.partition}:${receipt.offset}`
    records.set(id, { ...records.get(id), ...receipt, id })
  }
  return [...records.values()].sort(
    (first, second) =>
      Date.parse(second.timestamp) - Date.parse(first.timestamp),
  )
}

export function formatTime(timestamp?: string | null, includeDate = false) {
  if (!timestamp) return '--'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    ...(includeDate ? ({ day: '2-digit', month: 'short' } as const) : {}),
  }).format(new Date(timestamp))
}

export function totalLag(group?: Group) {
  return (
    group?.partitions.reduce((total, partition) => total + partition.lag, 0) ??
    0
  )
}
