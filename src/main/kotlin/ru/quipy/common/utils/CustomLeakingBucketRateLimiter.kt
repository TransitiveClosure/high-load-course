package ru.quipy.common.utils
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class CustomLeakingBucketRateLimiter(
    private val ratePerSecond: Int,
    private val maxQueueSize: Int
)  {
    private val privilegedQueue: Queue<CompletableFuture<Unit>> = LinkedList()
    private val normalQueue: Queue<CompletableFuture<Unit>> = LinkedList()
    private val lock = ReentrantLock()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    init {
        val delayMillis = 1000L / ratePerSecond
        scheduler.scheduleAtFixedRate(::leak, 0, delayMillis, TimeUnit.MILLISECONDS)
    }

    private fun leak() {
        lock.withLock {
            val next = privilegedQueue.poll() ?: normalQueue.poll()
            next?.complete(Unit)
        }
    }

    fun tickBlocking(privileged: Boolean = false) {
        val future = CompletableFuture<Unit>()

        lock.withLock {
            if ((privilegedQueue.size + normalQueue.size) >= maxQueueSize) {
                throw RejectedExecutionException("RateLimiter bucket is full!")
            }

            if (privileged) {
                privilegedQueue.add(future)
            } else {
                normalQueue.add(future)
            }
        }
        future.get()
    }

    fun stop() {
        scheduler.shutdownNow()
        lock.withLock {
            (privilegedQueue + normalQueue).forEach {
                it.completeExceptionally(CancellationException("RateLimiter stopped!"))
            }
            privilegedQueue.clear()
            normalQueue.clear()
        }
    }
}