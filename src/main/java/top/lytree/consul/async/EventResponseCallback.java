package top.lytree.consul.async;

import top.lytree.consul.model.EventResponse;

/**
 * For Event API calls that support long-polling, this callback is used to handle
 * the result on success or failure for an async HTTP call.
 */
public interface EventResponseCallback {

    /**
     * Callback for a successful {@link top.lytree.consul.model.EventResponse}.
     *
     * @param EventResponse The Consul event response.
     */
    void onComplete(EventResponse EventResponse);

    /**
     * Callback for an unsuccessful request.
     *
     * @param throwable The exception thrown.
     */
    void onFailure(Throwable throwable);
}
