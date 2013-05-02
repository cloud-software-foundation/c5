/*
 * Copyright (C) 2013  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/*
 * Copyright (c) 2008-2010 by Jan Stender,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */

package org.xtreemfs.foundation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for threads representing a life cycle. It offers methods for
 * blocking other threads until a certain life cycle event has occurred. It
 * currently supports two life cycle-related events: startup and shutdown.
 * 
 * @author stender
 * 
 */
public class LifeCycleThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(LifeCycleThread.class);
    
    private final Object      startLock;
    
    private final Object      stopLock;
    
    private boolean           started;
    
    private boolean           stopped;
    
    private Exception         exc;
    
    private LifeCycleListener listener;
    
    public LifeCycleThread(String name) {
    super(name);
    startLock = new Object();
        stopLock = new Object();
    }
    
    /**
     * This method should be invoked by subclasses when the startup procedure
     * has been completed.
     */
    protected void notifyStarted() {
        
        LOG.info("Thread {} started", Thread.currentThread().getName());
        
        synchronized (startLock) {
            started = true;
            startLock.notifyAll();
            if (listener != null)
                listener.startupPerformed();
        }
    }
    
    /**
     * This method should be invoked by subclasses when the shutdown procedure
     * has been completed.
     */
    protected void notifyStopped() {
        
        LOG.info("Thread {} terminated", Thread.currentThread().getName());
        
        synchronized (stopLock) {
            stopped = true;
            stopLock.notifyAll();
            if (listener != null)
                listener.shutdownPerformed();
        }
    }
    
    /**
     * This method should be invoked by subclasses when the thread has crashed.
     */
    protected void notifyCrashed(Throwable exc) {
        
        LOG.error("service ***CRASHED***, shutting down", exc);

        synchronized (startLock) {
            this.exc = exc instanceof Exception ? (Exception) exc : new Exception(exc);
            started = true;
            startLock.notifyAll();
        }
        
        synchronized (stopLock) {
            this.exc = exc instanceof Exception ? (Exception) exc : new Exception(exc);
            stopped = true;
            stopLock.notifyAll();
        }
        
        if (listener != null)
            listener.crashPerformed(exc);
    }
    
    /**
     * Synchronously waits for a notification indicating that the startup
     * procedure has been completed.
     * 
     * @throws Exception
     *             if an error occurred during the startup procedure
     */
    public void waitForStartup() throws Exception {
        synchronized (startLock) {
            
            while (!started)
                startLock.wait();
            
            if (exc != null && listener == null)
                throw exc;
        }
    }
    
    /**
     * Synchronously waits for a notification indicating that the shutdown
     * procedure has been completed.
     * 
     * @throws Exception
     *             if an error occurred during the shutdown procedure
     */
    public void waitForShutdown() throws Exception {
        synchronized (stopLock) {
            
            if (!started)
                return;
            while (!stopped)
                try {
                    stopLock.wait();
                } catch (InterruptedException e) {
                    // In case this thread executes notifyCrashed(), he will
                    // probably interrupt itself. However, this should not
                    // interfere with the notifyCrashed() procedure and
                    // therefore we swallow this exception.
                    if (listener == null) {
                        throw e;
                    }
                }
            
            if (exc != null && listener == null)
                throw exc;
        }
    }
    
    /**
     * Terminates the thread. This method should be overridden in subclasses.
     * @throws Exception if an error occurred
     */
    public void shutdown() throws Exception {
    }
    
    /**
     * Sets a listener waiting for life cycle events.
     * 
     * @param listener
     *            the listener
     */
    public void setLifeCycleListener(LifeCycleListener listener) {
        this.listener = listener;
    }
    
}
