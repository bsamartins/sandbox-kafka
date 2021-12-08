package io.bsamartins.sandbox.kafka.kstreams

data class Record<T>(val operation: Operation, val id: Int, val before: T?, val after: T?) {
    companion object {
        fun <T> add(id: Int, after: T): Record<T> = Record(Operation.ADD, id, null, after)
        fun <T> update(id: Int, before: T, after: T): Record<T> = Record(Operation.UPDATE, id, before, after)
        fun <T> delete(id: Int, before: T): Record<T> = Record(Operation.DELETE, id, before, null)
    }
}

enum class Operation {
    ADD,
    UPDATE,
    DELETE,
}
