package ru.quipy.common.utils

import java.lang.Math.ceil
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock

class StatisticsService (defaultAverageValue: Double) {
    private val times = LinkedList<Long>()
    private val lock = ReentrantReadWriteLock()
    private var percentile90: Double = 0.0
    private var maxLength: Int = 1000
    private var standardDeviation: Double = 0.0
    private var averageValue: Double = defaultAverageValue

    fun addProcessingTime(time: Long) {
        lock.writeLock().lock()
        try {
            if (times.size >= maxLength) {
                times.removeFirst()
            }
            times.add(time)
            recalculateStatistics()
        } finally {
            lock.writeLock().unlock()
        }
    }

    fun get90thPercentile(): Double {
        lock.readLock().lock()
        try {
            return percentile90
        } finally {
            lock.readLock().unlock()
        }
    }

    fun get95thPercentValue(): Double {
        lock.readLock().lock()
        try {
            return averageValue + 2 * standardDeviation
        } finally {
            lock.readLock().unlock()
        }
    }

    private fun recalculateStatistics() {
        if (times.isEmpty()) {
            percentile90 = 0.0
            standardDeviation = 0.0
            return
        }

        val sortedTimes = times.sorted()
        val index = kotlin.math.ceil(0.9 * sortedTimes.size).toInt() - 1
        percentile90 = sortedTimes[index].toDouble()

        averageValue = times.average()
        val variance = times.map { (it - averageValue) * (it - averageValue) }.average()
        standardDeviation = kotlin.math.sqrt(variance)
    }
}