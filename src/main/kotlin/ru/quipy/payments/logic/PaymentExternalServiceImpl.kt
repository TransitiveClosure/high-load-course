package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.StatisticsService
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.pow


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder().executor(Executors.newFixedThreadPool(parallelRequests)).version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofMillis(50000)).build()
    private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1, TimeUnit.SECONDS)

    private val semaphore = Semaphore(parallelRequests)
    private val statisticsService = StatisticsService(requestAverageProcessingTime.toMillis().toDouble());
    private var retryLimit = 2

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }


        val request =  HttpRequest.newBuilder()
            .uri(URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofMillis(20000))
            .build()
        rateLimiter.tickBlocking()
        semaphore.acquire()
        sendRequestWithRetry(request, paymentId, transactionId, accountName, now(), deadline, retryLimit, 10)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun now() = System.currentTimeMillis()

    fun isDeadlineWillExpired(deadlineTimeMillis: Long): Boolean  {
        return now() + 24000 >= deadlineTimeMillis
    }


    fun sendRequestWithRetry(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        accountName: String,
        start: Long,
        deadline: Long,
        retryCount: Int = 3,
        delayBetweenRetriesMs: Long = 10
    ): CompletableFuture<Unit> {
        val promise = CompletableFuture<Unit>()
        fun attempt(attemptNum: Int) {
                client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, throwable ->
                    if (throwable != null) {
                        logger.error(
                            "[$accountName] Exception on attempt $attemptNum for txId: $transactionId, payment: $paymentId",
                            throwable
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = throwable.message)
                        }

                        if (attemptNum < retryCount) {
                            CompletableFuture.delayedExecutor(delayBetweenRetriesMs, TimeUnit.MILLISECONDS).execute {

                                attempt(attemptNum + 1)
                            }
                        } else {
                            promise.complete(Unit)
                        }

                    } else {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId", e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.statusCode()}")

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        if (body.result) {
                            promise.complete(Unit)
                        } else if (attemptNum < retryCount) {
                            if (isDeadlineWillExpired(deadline)) {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(success = false, now(), transactionId, reason = "Deadline will expired before request")
                                }
                                promise.complete(Unit)
                            }
                            else
                            {
                                CompletableFuture.delayedExecutor(delayBetweenRetriesMs, TimeUnit.MILLISECONDS).execute {
                                    attempt(attemptNum + 1)
                                }
                            }
                        } else {
                            promise.complete(Unit)
                        }
                    }
                }
            }

        attempt(1)
        logger.info("StatValues: ${statisticsService.get95thPercentile()}, ${statisticsService.get95thPercentValue()}")
        semaphore.release()
        return promise
    }
}
