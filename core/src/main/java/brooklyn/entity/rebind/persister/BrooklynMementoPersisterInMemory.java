package brooklyn.entity.rebind.persister;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.proxying.EntityProxy;
import brooklyn.location.Location;
import brooklyn.location.basic.LocationInternal;
import brooklyn.mementos.BrooklynMemento;
import brooklyn.util.os.Os;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

public class BrooklynMementoPersisterInMemory extends AbstractBrooklynMementoPersister {

    private final ClassLoader classLoader;
    private final boolean checkPersistable;
    
    public BrooklynMementoPersisterInMemory(ClassLoader classLoader) {
        this(classLoader, true);
    }
    
    public BrooklynMementoPersisterInMemory(ClassLoader classLoader, boolean checkPersistable) {
        this.classLoader = checkNotNull(classLoader, "classLoader");
        this.checkPersistable = checkPersistable;
    }
    
    @VisibleForTesting
    @Override
    public void waitForWritesCompleted(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        // TODO Could wait for concurrent checkpoint/delta, but don't need to for tests
        // because first waits for checkpoint/delta to have been called by RebindManagerImpl.
        return;
    }

    @Override
    public void checkpoint(BrooklynMemento newMemento) {
        super.checkpoint(newMemento);
        if (checkPersistable) reserializeMemento();
    }

    @Override
    public void delta(Delta delta) {
        super.delta(delta);
        if (checkPersistable) reserializeMemento();
    }
    
    private void reserializeMemento() {
        // To confirm always serializable
        try {
            File tempDir = Files.createTempDir();
            try {
                // TODO See RebindManagerImpl.rebind for dummyLookupContext; remove duplication
                BrooklynMementoPersisterToMultiFile persister = new BrooklynMementoPersisterToMultiFile(tempDir , classLoader);
                persister.checkpoint(memento);
                LookupContext dummyLookupContext = new LookupContext() {
                    private final Entity dummyEntity = (Entity) java.lang.reflect.Proxy.newProxyInstance(
                            classLoader,
                            new Class[] {Entity.class, EntityInternal.class, EntityProxy.class},
                            new InvocationHandler() {
                                @Override public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
                                    return m.invoke(this, args);
                                }
                            });
                    private final Location dummyLocation = (Location) java.lang.reflect.Proxy.newProxyInstance(
                            classLoader,
                            new Class[] {Location.class, LocationInternal.class},
                            new InvocationHandler() {
                                @Override public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
                                    return m.invoke(this, args);
                                }
                            });
                    @Override public Entity lookupEntity(String id) {
                        return dummyEntity;
                    }
                    @Override public Location lookupLocation(String id) {
                        return dummyLocation;
                    }
                };

                // Not actually reconstituting, because need to use a realy lookupContext to reconstitute all the entities
                BrooklynMemento reloadedMemento = persister.loadMemento(dummyLookupContext);
            } finally {
                Os.tryDeleteDirectory(tempDir.getAbsolutePath());
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
