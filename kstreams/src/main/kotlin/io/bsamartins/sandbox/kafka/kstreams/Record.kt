package io.bsamartins.sandbox.kafka.kstreams

data class Record<T>(val operation: Operation, val id: String, val before: T?, val after: T?) {
    companion object {
        fun <T> add(id: String, after: T): Record<T> = Record(Operation.ADD, id, null, after)
        fun <T> update(id: String, before: T, after: T): Record<T> = Record(Operation.UPDATE, id, before, after)
        fun <T> delete(id: String, before: T): Record<T> = Record(Operation.DELETE, id, before, null)
    }
}

enum class Operation {
    ADD,
    UPDATE,
    DELETE,
}
