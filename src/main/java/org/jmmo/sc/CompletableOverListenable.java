package org.jmmo.sc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;

public class CompletableOverListenable<T> extends CompletableFuture<T> implements FutureCallback<T> {
    private final ListenableFuture<T> listenableFuture;

    public CompletableOverListenable(ListenableFuture<T> listenableFuture) {
        this.listenableFuture = listenableFuture;

        Futures.addCallback(listenableFuture, this);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = listenableFuture.cancel(mayInterruptIfRunning);
        super.cancel(mayInterruptIfRunning);
        return result;
    }

    @Override public void onSuccess(T result) {
        complete(result);
    }

    @Override public void onFailure(Throwable t) {
        completeExceptionally(t);
    }
}
