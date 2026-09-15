import { useEffect, useRef } from 'react'
import type { ReactNode } from 'react'
import {
  Activity,
  ArrowRight,
  Bell,
  Box,
  Check,
  CircleHelp,
  Database,
  GitBranch,
  Layers3,
  Package,
  Pause,
  Play,
  Radio,
  ShieldCheck,
  ShoppingBag,
  X,
} from 'lucide-react'
import { serviceLabels, services, totalLag } from './data'
import type {
  ConsumerSnapshot,
  DashboardData,
  Group,
  Outcome,
  Partition,
  Service,
} from './data'

export function IconButton({
  label,
  children,
  onClick,
  disabled,
  className = '',
}: {
  label: string
  children: ReactNode
  onClick: () => void
  disabled?: boolean
  className?: string
}) {
  return (
    <button
      type="button"
      className={`icon-button ${className}`}
      aria-label={label}
      title={label}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  )
}

export function OutcomeBadge({ outcome }: { outcome?: Outcome }) {
  const label = {
    PROCESSED: 'Processed',
    DUPLICATE: 'Duplicate skipped',
    CONFLICT: 'Conflict blocked',
  }
  return (
    <span className={`outcome ${outcome?.toLowerCase() ?? 'pending'}`}>
      {outcome === 'PROCESSED' ? (
        <Check size={12} />
      ) : outcome ? (
        <ShieldCheck size={12} />
      ) : (
        <span className="status-dot" />
      )}
      {outcome ? label[outcome] : 'Awaiting consumer'}
    </span>
  )
}

export function ConsumerNode({
  service,
  snapshot,
  group,
  error,
  busy,
  onPause,
}: {
  service: Service
  snapshot: ConsumerSnapshot | null
  group?: Group
  error?: string
  busy?: boolean
  onPause: (service: Service) => void
}) {
  const ServiceIcon = service === 'inventory' ? Package : Bell
  const state = error
    ? 'Unavailable'
    : !snapshot
      ? 'Connecting'
      : snapshot.paused
        ? 'Paused'
        : snapshot.pauseRequested
          ? 'Pausing'
          : snapshot.running
            ? 'Consuming'
            : 'Stopped'
  return (
    <div className={`consumer-node ${service}`}>
      <div className="node-heading">
        <span className="node-icon">
          <ServiceIcon size={17} />
        </span>
        <strong>{serviceLabels[service]}</strong>
        <IconButton
          label={`${snapshot?.pauseRequested ? 'Resume' : 'Pause'} ${service} consumer`}
          disabled={!snapshot || !!error || busy}
          onClick={() => onPause(service)}
        >
          {snapshot?.pauseRequested ? <Play size={14} /> : <Pause size={14} />}
        </IconButton>
      </div>
      <code className="group-code">{service}-group</code>
      <div className="node-status">
        <span
          className={`status-dot ${error ? 'bad' : snapshot?.pauseRequested ? 'amber' : 'green'}`}
        />
        {state}
        <span className="node-lag">{group ? totalLag(group) : '--'} lag</span>
      </div>
      <div className="database-foot">
        <Database size={13} />
        <span>{service}-db</span>
        <b>{snapshot?.counts.processed ?? '--'}</b>
      </div>
    </div>
  )
}

export function OffsetRail({
  partition,
  compact = false,
}: {
  partition: Partition
  compact?: boolean
}) {
  const count = Math.min(
    compact ? 6 : 10,
    partition.endOffset - partition.startOffset,
  )
  return (
    <div
      className={`offset-rail ${compact ? 'compact' : ''}`}
      aria-label={`Partition ${partition.id} ends at offset ${partition.endOffset}`}
    >
      {Array.from({ length: compact ? 6 : 10 }, (_, index) => {
        const offset = partition.endOffset - count + index
        return (
          <span
            key={index}
            className={
              index < count
                ? `offset-slot filled partition-color-${partition.id % 3}`
                : 'offset-slot'
            }
            title={
              index < count ? `Record offset ${offset}` : 'No retained record'
            }
          >
            {index < count && !compact ? offset : ''}
          </span>
        )
      })}
    </div>
  )
}

export function Topology({
  data,
  publication,
  busy,
  onPause,
  onPartition,
}: {
  data: DashboardData
  publication: string
  busy: Service | null
  onPause: (service: Service) => void
  onPartition: (partition: number) => void
}) {
  return (
    <section
      className="topology-section"
      aria-label="Live Kafka event topology"
    >
      <div className="section-heading">
        <h2>
          <GitBranch size={17} /> Live topology
        </h2>
        <span className="quiet-label">
          Producer to independent consumer groups
        </span>
      </div>
      <div className="topology-canvas">
        <svg
          className="flow-wires"
          viewBox="0 0 1000 340"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          <path d="M 180 170 H 380" />
          <path d="M 580 170 H 730 V 85 H 850" />
          <path d="M 730 170 V 255 H 850" />
          {publication && (
            <circle key={publication} r="5" fill="#367bdb">
              <animateMotion dur="1.4s" path="M 180 170 H 380" fill="freeze" />
            </circle>
          )}
        </svg>
        <div className="producer-node">
          <span className="producer-symbol">
            <ShoppingBag size={24} strokeWidth={1.6} />
          </span>
          <strong>Order service</strong>
          <span className="small-muted">Event producer</span>
          <code>POST /create</code>
          <div className="node-status">
            <span
              className={`status-dot ${data.errors.kafka ? 'bad' : 'green'}`}
            />
            Port 4003
          </div>
        </div>
        <div className="topic-node">
          <div className="topic-heading">
            <span className="kafka-symbol">
              <Radio size={17} />
            </span>
            <div>
              <strong>order-events</strong>
              <span>Kafka topic</span>
            </div>
          </div>
          <div className="topic-partitions">
            {data.kafka?.partitions.map((partition) => (
              <button
                key={partition.id}
                type="button"
                className="mini-partition"
                onClick={() => onPartition(partition.id)}
                title={`Inspect partition ${partition.id}`}
              >
                <code>P{partition.id}</code>
                <OffsetRail partition={partition} compact />
                <span>{partition.endOffset}</span>
              </button>
            )) ?? (
              <div className="empty-compact">Waiting for broker metadata</div>
            )}
          </div>
          <div className="topic-foot">
            <Layers3 size={12} />
            {data.kafka?.partitions.length ?? '--'} partitions
            <span>Key: orderId</span>
          </div>
        </div>
        <div className="consumer-nodes">
          {services.map((service) => (
            <ConsumerNode
              key={service}
              service={service}
              snapshot={data[service]}
              group={data.kafka?.groups.find(
                (group) => group.service === service,
              )}
              error={data.errors[service]}
              busy={busy === service}
              onPause={onPause}
            />
          ))}
        </div>
      </div>
      <div className="topology-caption">
        <span>
          <span className="legend-line blue" />
          Published event
        </span>
        <span>
          <span className="legend-line" />
          Independent delivery
        </span>
        <span>
          <Database size={12} />
          Durable processing
        </span>
      </div>
    </section>
  )
}

export function PartitionExplorer({
  data,
  onHistory,
}: {
  data: DashboardData
  onHistory: (partition: number) => void
}) {
  return (
    <>
      <div className="insight-band">
        <GitBranch size={20} />
        <p>
          A key chooses a partition. An offset identifies a record{' '}
          <strong>within that partition</strong>. Each consumer group tracks its
          own position.
        </p>
      </div>
      <div className="partition-grid">
        {data.kafka?.partitions.map((partition) => (
          <section
            className={`partition-card partition-color-${partition.id % 3}`}
            key={partition.id}
          >
            <div className="section-heading">
              <h2>
                <Layers3 size={18} />
                Partition {partition.id}
              </h2>
              <span className="tiny-tag">Leader {partition.leader}</span>
            </div>
            <div className="partition-number">
              {partition.endOffset}
              <span>next offset</span>
            </div>
            <OffsetRail partition={partition} />
            <div className="rail-labels">
              <span>Start {partition.startOffset}</span>
              <span>
                {partition.endOffset - partition.startOffset} retained
              </span>
            </div>
            <div className="partition-positions">
              {services.map((service) => {
                const position = data.kafka?.groups
                  .find((group) => group.service === service)
                  ?.partitions.find((position) => position.id === partition.id)
                const progress = position
                  ? Math.max(
                      0,
                      Math.min(
                        100,
                        (100 *
                          ((position.committedOffset ?? partition.startOffset) -
                            partition.startOffset)) /
                          Math.max(
                            1,
                            partition.endOffset - partition.startOffset,
                          ),
                      ),
                    )
                  : 0
                return (
                  <div className={`position-line ${service}`} key={service}>
                    <div>
                      <span>{serviceLabels[service]}</span>
                      <code>
                        {position?.committedOffset ?? '--'} /{' '}
                        {partition.endOffset}
                      </code>
                    </div>
                    <div className="position-track">
                      <span style={{ width: `${progress}%` }} />
                    </div>
                    <small>
                      {position
                        ? `${position.lag} records behind`
                        : 'No committed offset yet'}
                    </small>
                  </div>
                )
              })}
            </div>
            <div className="partition-foot">
              <span>
                {partition.inSyncReplicas.length}/{partition.replicas.length}{' '}
                replicas in sync
              </span>
              <button
                className="text-button"
                onClick={() => onHistory(partition.id)}
              >
                Deliveries <ArrowRight size={14} />
              </button>
            </div>
          </section>
        ))}
      </div>
      {!data.kafka && (
        <EmptyState
          title="Kafka metadata is not available"
          text="The partition view will appear when the broker responds."
        />
      )}
    </>
  )
}

export function GroupExplorer({
  data,
  busy,
  onPause,
}: {
  data: DashboardData
  busy: Service | null
  onPause: (service: Service) => void
}) {
  return (
    <>
      <div className="insight-band">
        <ShieldCheck size={20} />
        <p>
          Different group IDs mean{' '}
          <strong>both services receive every event</strong>. Pausing one
          consumer leaves the other free to continue.
        </p>
      </div>
      <div className="group-grid">
        {services.map((service) => {
          const snapshot = data[service]
          const group = data.kafka?.groups.find(
            (group) => group.service === service,
          )
          const ServiceIcon = service === 'inventory' ? Package : Bell
          return (
            <section className={`group-card ${service}`} key={service}>
              <div className="section-heading">
                <h2>
                  <ServiceIcon size={20} />
                  {serviceLabels[service]} group
                </h2>
                <span
                  className={`tiny-tag ${snapshot?.pauseRequested ? 'warning' : ''}`}
                >
                  {snapshot?.paused
                    ? 'Paused'
                    : snapshot?.pauseRequested
                      ? 'Pausing'
                      : (group?.state ?? 'Unknown')}
                </span>
              </div>
              <code className="group-identity">{service}-group</code>
              <div className="group-metrics">
                <div>
                  <strong>{group?.members.length ?? '--'}</strong>
                  <span>active members</span>
                </div>
                <div>
                  <strong>{group ? totalLag(group) : '--'}</strong>
                  <span>total lag</span>
                </div>
                <div>
                  <strong>{snapshot?.counts.processed ?? '--'}</strong>
                  <span>orders saved</span>
                </div>
              </div>
              <div className="table-scroll">
                <table className="offset-table">
                  <thead>
                    <tr>
                      <th>Partition</th>
                      <th>Committed</th>
                      <th>Log end</th>
                      <th>Lag</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group?.partitions.map((partition) => (
                      <tr key={partition.id}>
                        <td>
                          <code>P{partition.id}</code>
                        </td>
                        <td>{partition.committedOffset ?? '--'}</td>
                        <td>{partition.endOffset}</td>
                        <td
                          className={
                            partition.lag ? 'warning-text' : 'success-text'
                          }
                        >
                          {partition.lag}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="member-list">
                <span className="eyebrow">MEMBER ASSIGNMENTS</span>
                {group?.members.map((member) => (
                  <div key={member.id}>
                    <Activity size={14} />
                    <code>{member.clientId}</code>
                    <span>
                      {member.partitions
                        .map((partition) => `P${partition}`)
                        .join(', ')}
                    </span>
                  </div>
                ))}
                {!group?.members.length && (
                  <p className="small-muted">
                    No active member reported by Kafka.
                  </p>
                )}
              </div>
              <div className="group-footer">
                <span>
                  <Database size={14} />
                  {service}-db / PostgreSQL
                </span>
                <button
                  className="secondary-button"
                  disabled={
                    !snapshot || !!data.errors[service] || busy === service
                  }
                  onClick={() => onPause(service)}
                >
                  {snapshot?.pauseRequested ? (
                    <Play size={14} />
                  ) : (
                    <Pause size={14} />
                  )}
                  {snapshot?.pauseRequested ? 'Resume' : 'Pause'} consumer
                </button>
              </div>
            </section>
          )
        })}
      </div>
    </>
  )
}

export function EmptyState({ title, text }: { title: string; text: string }) {
  return (
    <div className="empty-state">
      <Box size={30} strokeWidth={1.3} />
      <strong>{title}</strong>
      <p>{text}</p>
    </div>
  )
}

export function Modal({
  title,
  children,
  onClose,
  className = '',
}: {
  title: string
  children: ReactNode
  onClose: () => void
  className?: string
}) {
  const dialog = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    dialog.current?.showModal()
  }, [])
  return (
    <dialog
      ref={dialog}
      className={`modal ${className}`}
      aria-labelledby="modal-title"
      onCancel={onClose}
      onClick={(event) => {
        if (event.target === dialog.current) onClose()
      }}
    >
      <div className="modal-heading">
        <h2 id="modal-title">{title}</h2>
        <IconButton label="Close dialog" onClick={onClose}>
          <X size={19} />
        </IconButton>
      </div>
      {children}
    </dialog>
  )
}

export function Guide({ onClose }: { onClose: () => void }) {
  return (
    <Modal
      title="A small guide to the event flow"
      onClose={onClose}
      className="guide-modal"
    >
      <p className="guide-intro">
        One order travels through Kafka to two independent consumers. Here is
        what the numbers mean.
      </p>
      {[
        [
          ShoppingBag,
          '01',
          'Publish',
          'The order API sends ORDER_CREATED with orderId and itemName. A successful response means Kafka acknowledged the write, not that both consumers have finished.',
        ],
        [
          Layers3,
          '02',
          'Partition & key',
          'The integer orderId is the record key. Kafka hashes it to a partition. The same key keeps the same partition while the partition count is unchanged.',
        ],
        [
          GitBranch,
          '03',
          'Consumer groups',
          'Inventory and notification use different group IDs, so each receives every event. Members inside one group divide its partitions between them.',
        ],
        [
          Activity,
          '04',
          'Offset & lag',
          'A committed offset is the next record a group should read. Lag is the distance from that position to the partition log end. Pausing a consumer lets you observe lag building up.',
        ],
        [
          ShieldCheck,
          '05',
          'Idempotency',
          'Each database has a unique (orderId, eventType) constraint. The first delivery saves an order, an identical replay is skipped, and a changed item is flagged as a conflict. The order and delivery audit commit in one database transaction before the Kafka offset is acknowledged.',
        ],
      ].map(([Icon, number, title, text]) => {
        const GuideIcon = Icon as typeof CircleHelp
        return (
          <section className="guide-entry" key={String(number)}>
            <span>
              <GuideIcon size={20} />
            </span>
            <div>
              <small>{String(number)}</small>
              <h3>{String(title)}</h3>
              <p>{String(text)}</p>
            </div>
          </section>
        )
      })}
      <div className="guide-foot">
        <Database size={17} />
        <p>
          This local demo records processing in PostgreSQL. It does not send
          real notifications or modify stock. Database trust authentication is
          for local development only.
        </p>
      </div>
    </Modal>
  )
}
