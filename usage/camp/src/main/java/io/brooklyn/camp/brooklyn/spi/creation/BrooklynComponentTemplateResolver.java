/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.brooklyn.camp.brooklyn.spi.creation;

import io.brooklyn.camp.brooklyn.BrooklynCampConstants;
import io.brooklyn.camp.brooklyn.spi.creation.service.BrooklynServiceTypeResolver;
import io.brooklyn.camp.brooklyn.spi.creation.service.ServiceTypeResolver;
import io.brooklyn.camp.spi.AbstractResource;
import io.brooklyn.camp.spi.ApplicationComponentTemplate;
import io.brooklyn.camp.spi.AssemblyTemplate;
import io.brooklyn.camp.spi.PlatformComponentTemplate;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.catalog.CatalogItem;
import brooklyn.catalog.internal.CatalogUtils;
import brooklyn.config.ConfigKey;
import brooklyn.entity.Application;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.AbstractEntity;
import brooklyn.entity.basic.BrooklynTags;
import brooklyn.entity.basic.BrooklynTaskTags;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.entity.proxying.InternalEntityFactory;
import brooklyn.location.Location;
import brooklyn.management.ManagementContext;
import brooklyn.management.ManagementContextInjectable;
import brooklyn.management.classloading.BrooklynClassLoadingContext;
import brooklyn.management.classloading.JavaBrooklynClassLoadingContext;
import brooklyn.management.internal.ManagementContextInternal;
import brooklyn.util.collections.MutableList;
import brooklyn.util.collections.MutableSet;
import brooklyn.util.config.ConfigBag;
import brooklyn.util.flags.FlagUtils;
import brooklyn.util.flags.FlagUtils.FlagConfigKeyAndValueRecord;
import brooklyn.util.guava.Maybe;
import brooklyn.util.javalang.Reflections;
import brooklyn.util.task.Tasks;
import brooklyn.util.text.Strings;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * This generates instances of a template resolver that use a {@link ServiceTypeResolver}
 * to parse the {@code serviceType} line in the template.
 */
public class BrooklynComponentTemplateResolver {

    private static final Logger log = LoggerFactory.getLogger(BrooklynComponentTemplateResolver.class);

    private final BrooklynClassLoadingContext loader;
    private final ManagementContext mgmt;
    private final ConfigBag attrs;
    private final Maybe<AbstractResource> template;
    private final BrooklynYamlTypeInstantiator.Factory yamlLoader;
    private final String type;
    private final ServiceTypeResolver typeResolver;
    private final AtomicBoolean alreadyBuilt = new AtomicBoolean(false);

    public BrooklynComponentTemplateResolver(BrooklynClassLoadingContext loader, ConfigBag attrs, AbstractResource optionalTemplate, String type, ServiceTypeResolver typeResolver) {
        this.loader = loader;
        this.mgmt = loader.getManagementContext();
        this.attrs = ConfigBag.newInstanceCopying(attrs);
        this.template = Maybe.fromNullable(optionalTemplate);
        this.yamlLoader = new BrooklynYamlTypeInstantiator.Factory(loader, this);
        this.type = type;
        this.typeResolver = typeResolver;
    }

    public BrooklynClassLoadingContext getLoader() { return loader; }
    public ManagementContext getManagementContext() { return mgmt; }
    public ConfigBag getAttrs() { return attrs; }
    public Maybe<AbstractResource> getTemplate() { return template; }
    public BrooklynYamlTypeInstantiator.Factory getYamlLoader() { return yamlLoader; }
    public ServiceTypeResolver getServiceTypeResolver() { return typeResolver; }
    public String getDeclaredType() { return type; }
    public Boolean isAlreadyBuilt() { return alreadyBuilt.get(); }

    public static class Factory {

        /** returns resolver type based on the service type, inspecting the arguments in order to determine the service type */
        private static ServiceTypeResolver computeResolverType(BrooklynClassLoadingContext context, String knownServiceType, AbstractResource optionalTemplate, ConfigBag attrs) {
            String type = getDeclaredType(knownServiceType, optionalTemplate, attrs);
            return findService(context, type);
        }

        // TODO This could be extended to support multiple prefixes per resolver and a 'best-match' algorithm
        protected static ServiceTypeResolver findService(BrooklynClassLoadingContext context, String type) {
            if (type.indexOf(':') != -1) {
                String prefix = Splitter.on(":").splitToList(type).get(0);
                ServiceLoader<ServiceTypeResolver> loader = ServiceLoader.load(ServiceTypeResolver.class,
                        context.getManagementContext().getCatalog().getRootClassLoader());
                for (ServiceTypeResolver resolver : loader) {
                   if (prefix.equals(resolver.getTypePrefix())) {
                       return resolver;
                   }
                }
            }
            return null;
        }

        public static BrooklynComponentTemplateResolver newInstance(BrooklynClassLoadingContext context, Map<String, ?> childAttrs) {
            return newInstance(context, ConfigBag.newInstance(childAttrs), null);
        }

        public static BrooklynComponentTemplateResolver newInstance(BrooklynClassLoadingContext context, AbstractResource template) {
            return newInstance(context, ConfigBag.newInstance(template.getCustomAttributes()), template);
        }

        public static BrooklynComponentTemplateResolver newInstance(BrooklynClassLoadingContext context, String serviceType) {
            return newInstance(context, ConfigBag.newInstance().configureStringKey("serviceType", serviceType), null);
        }

        private static BrooklynComponentTemplateResolver newInstance(BrooklynClassLoadingContext context, ConfigBag attrs, AbstractResource optionalTemplate) {
            ServiceTypeResolver typeResolver = computeResolverType(context, null, optionalTemplate, attrs);
            String type = getDeclaredType(null, optionalTemplate, attrs);
            if (typeResolver == null) // use default
                typeResolver = new BrooklynServiceTypeResolver();
            return new BrooklynComponentTemplateResolver(context, attrs, optionalTemplate, type, typeResolver);
        }

        public static String getDeclaredType(String knownServiceType, AbstractResource optionalTemplate, @Nullable ConfigBag attrs) {
            String type = knownServiceType;
            if (type==null && optionalTemplate!=null) {
                type = optionalTemplate.getType();
                if (type.equals(AssemblyTemplate.CAMP_TYPE) || type.equals(PlatformComponentTemplate.CAMP_TYPE) || type.equals(ApplicationComponentTemplate.CAMP_TYPE))
                    // ignore these values for the type; only subclasses are interesting
                    type = null;
            }
            if (type==null) type = extractServiceTypeAttribute(attrs);
            return type;
        }

        private static String extractServiceTypeAttribute(@Nullable ConfigBag attrs) {
            return BrooklynYamlTypeInstantiator.InstantiatorFromKey.extractTypeName("service", attrs).orNull();
        }

        public static boolean supportsType(BrooklynClassLoadingContext context, String serviceType) {
            ServiceTypeResolver typeResolver = computeResolverType(context, serviceType, null, null);
            if (typeResolver != null) return true;
            return newInstance(context, serviceType).canResolve();
        }
    }

    protected boolean canResolve() {
        if (typeResolver.getCatalogItem(this, type)!=null)
            return true;
        if (loader.tryLoadClass(getJavaType(), Entity.class).isPresent())
            return true;
        return false;
    }

    /** returns the entity class, if needed in contexts which scan its statics for example */
    protected Class<? extends Entity> loadEntityClass() {
        Maybe<Class<? extends Entity>> result = tryLoadEntityClass();
        if (result.isAbsent())
            throw new IllegalStateException("Could not find "+typeResolver.getBrooklynType(type), ((Maybe.Absent<?>)result).getException());
        return result.get();
    }

    /** tries to load the Java entity class */
    protected Maybe<Class<? extends Entity>> tryLoadEntityClass() {
        return loader.tryLoadClass(getJavaType(), Entity.class);
    }

    // TODO Generalise to have other prefixes (e.g. explicit "catalog:" etc)?
    protected boolean isJavaTypePrefix() {
        return type != null && (type.toLowerCase().startsWith("java:") || type.toLowerCase().startsWith("brooklyn:java:"));
    }

    protected String getJavaType() {
        CatalogItem<Entity, EntitySpec<?>> item = typeResolver.getCatalogItem(this, type);
        if (!isJavaTypePrefix() && item != null && item.getJavaType() != null) {
            return item.getJavaType();
        } else {
            return typeResolver.getBrooklynType(type);
        }
    }

    /** resolves the spec, updating the loader if a catalog item is loaded */
    protected <T extends Entity> EntitySpec<T> resolveSpec() {
        if (alreadyBuilt.getAndSet(true))
            throw new IllegalStateException("Spec can only be used once: "+this);

        EntitySpec<T> spec = createSpec();
        populateSpec(spec);

        return spec;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected <T extends Entity> EntitySpec<T> createSpec() {
        Class<T> type = (Class<T>) loadEntityClass();

        EntitySpec<T> spec;
        if (type.isInterface()) {
            spec = EntitySpec.create(type);
        } else {
            // If this is a concrete class, particularly for an Application class, we want the proxy
            // to expose all interfaces it implements.
            Class interfaceclazz = (Application.class.isAssignableFrom(type)) ? Application.class : Entity.class;
            List<Class<?>> additionalInterfaceClazzes = Reflections.getAllInterfaces(type);
            spec = EntitySpec.create(interfaceclazz).impl(type).additionalInterfaces(additionalInterfaceClazzes);
        }
        spec.catalogItemId(CatalogUtils.getCatalogItemIdFromLoader(loader));
        if (template.isPresent() && template.get().getSourceCode()!=null)
            spec.tag(BrooklynTags.newYamlSpecTag(template.get().getSourceCode()));

        return spec;
    }

    //called from BrooklynAssemblyTemplateInstantiator as well
    @SuppressWarnings("unchecked")
    protected <T extends Entity> void populateSpec(EntitySpec<T> spec) {
        String name, templateId=null, planId=null;
        if (template.isPresent()) {
            name = template.get().getName();
            templateId = template.get().getId();
        } else {
            name = (String)attrs.getStringKey("name");
        }
        planId = (String)attrs.getStringKey("id");
        if (planId==null)
            planId = (String) attrs.getStringKey(BrooklynCampConstants.PLAN_ID_FLAG);

        Object childrenObj = attrs.getStringKey("brooklyn.children");
        if (childrenObj != null) {
            Set<String> encounteredCatalogTypes = MutableSet.of();

            Iterable<Map<String,?>> children = (Iterable<Map<String,?>>)childrenObj;
            for (Map<String,?> childAttrs : children) {
                BrooklynComponentTemplateResolver entityResolver = BrooklynComponentTemplateResolver.Factory.newInstance(loader, childAttrs);
                BrooklynAssemblyTemplateInstantiator instantiator = new BrooklynAssemblyTemplateInstantiator();
                // TODO: Creating a new set of encounteredCatalogTypes prevents the recursive definition check in
                // BrooklynAssemblyTemplateInstantiator.resolveSpec from correctly determining if a YAML entity is
                // defined recursively. However, the number of overrides of newInstance, and the number of places
                // calling populateSpec make it difficult to pass encounteredCatalogTypes in as a parameter
                EntitySpec<? extends Entity> childSpec = instantiator.resolveSpec(entityResolver, encounteredCatalogTypes);
                spec.child(childSpec);
            }
        }
        if (!Strings.isBlank(name))
            spec.displayName(name);
        if (templateId != null)
            spec.configure(BrooklynCampConstants.TEMPLATE_ID, templateId);
        if (planId != null)
            spec.configure(BrooklynCampConstants.PLAN_ID, planId);

        List<Location> childLocations = new BrooklynYamlLocationResolver(mgmt).resolveLocations(attrs.getAllConfig(), true);
        if (childLocations != null)
            spec.locations(childLocations);

        typeResolver.decorateSpec(this, spec);
        configureEntityConfig(spec);
    }

    /** returns new *uninitialised* entity, with just a few of the pieces from the spec;
     * initialisation occurs soon after, in {@link #initEntity(ManagementContext, Entity, EntitySpec)},
     * inside an execution context and after entity ID's are recognised
     */
    protected <T extends Entity> T newEntity(EntitySpec<T> spec) {
        Class<? extends T> entityImpl = (spec.getImplementation() != null) ? spec.getImplementation() : mgmt.getEntityManager().getEntityTypeRegistry().getImplementedBy(spec.getType());
        InternalEntityFactory entityFactory = ((ManagementContextInternal)mgmt).getEntityFactory();
        T entity = entityFactory.constructEntity(entityImpl, spec);

        String planId = (String)spec.getConfig().get(BrooklynCampConstants.PLAN_ID);
        if (planId != null) {
            entity.config().set(BrooklynCampConstants.PLAN_ID, planId);
        }

        if (spec.getLocations().size() > 0) {
            ((AbstractEntity)entity).addLocations(spec.getLocations());
        }

        if (spec.getParent() != null) entity.setParent(spec.getParent());

        return entity;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void configureEntityConfig(EntitySpec<?> spec) {
        ConfigBag bag = ConfigBag.newInstance((Map<Object, Object>) attrs.getStringKey("brooklyn.config"));

        // first take *recognised* flags and config keys from the top-level, and put them in the bag (of brooklyn.config)
        // (for component templates this will have been done already by BrooklynEntityMatcher, but for specs it is needed here)
        ConfigBag bagFlags = ConfigBag.newInstanceCopying(attrs);
        List<FlagConfigKeyAndValueRecord> topLevelApparentConfig = FlagUtils.findAllFlagsAndConfigKeys(null, spec.getType(), bagFlags);
        for (FlagConfigKeyAndValueRecord r: topLevelApparentConfig) {
            if (r.getConfigKeyMaybeValue().isPresent())
                bag.putIfAbsent((ConfigKey)r.getConfigKey(), r.getConfigKeyMaybeValue().get());
            if (r.getFlagMaybeValue().isPresent())
                bag.putAsStringKeyIfAbsent(r.getFlagName(), r.getFlagMaybeValue().get());
        }

        // now set configuration for all the items in the bag
        List<FlagConfigKeyAndValueRecord> records = FlagUtils.findAllFlagsAndConfigKeys(null, spec.getType(), bag);
        Set<String> keyNamesUsed = new LinkedHashSet<String>();
        for (FlagConfigKeyAndValueRecord r: records) {
            if (r.getFlagMaybeValue().isPresent()) {
                Object transformed = new SpecialFlagsTransformer(loader).transformSpecialFlags(r.getFlagMaybeValue().get());
                spec.configure(r.getFlagName(), transformed);
                keyNamesUsed.add(r.getFlagName());
            }
            if (r.getConfigKeyMaybeValue().isPresent()) {
                Object transformed = new SpecialFlagsTransformer(loader).transformSpecialFlags(r.getConfigKeyMaybeValue().get());
                spec.configure((ConfigKey<Object>)r.getConfigKey(), transformed);
                keyNamesUsed.add(r.getConfigKey().getName());
            }
        }

        // set unused keys as anonymous config keys -
        // they aren't flags or known config keys, so must be passed as config keys in order for
        // EntitySpec to know what to do with them (as they are passed to the spec as flags)
        for (String key: MutableSet.copyOf(bag.getUnusedConfig().keySet())) {
            // we don't let a flag with the same name as a config key override the config key
            // (that's why we check whether it is used)
            if (!keyNamesUsed.contains(key)) {
                Object transformed = new SpecialFlagsTransformer(loader).transformSpecialFlags(bag.getStringKey(key));
                spec.configure(ConfigKeys.newConfigKey(Object.class, key.toString()), transformed);
            }
        }
    }

    protected static class SpecialFlagsTransformer implements Function<Object, Object> {
        protected final ManagementContext mgmt;
        /* TODO find a way to make do without loader here?
         * it is not very nice having to serialize it; but serialization of BLCL is now relatively clean.
         *
         * it is only used to instantiate classes, and now most things should be registered with catalog;
         * the notable exception is when one entity in a bundle is creating another in the same bundle,
         * it wants to use his bundle CLC to do that.  but we can set up some unique reference to the entity
         * which can be used to find it from mgmt, rather than pass the loader.
         */
        private BrooklynClassLoadingContext loader = null;

        public SpecialFlagsTransformer(BrooklynClassLoadingContext loader) {
            this.loader = loader;
            mgmt = loader.getManagementContext();
        }
        public Object apply(Object input) {
            if (input instanceof Map)
                return transformSpecialFlags((Map<?, ?>)input);
            else if (input instanceof Set<?>)
                return MutableSet.of(transformSpecialFlags((Iterable<?>)input));
            else if (input instanceof List<?>)
                return MutableList.copyOf(transformSpecialFlags((Iterable<?>)input));
            else if (input instanceof Iterable<?>)
                return transformSpecialFlags((Iterable<?>)input);
            else
                return transformSpecialFlags((Object)input);
        }

        protected Map<?, ?> transformSpecialFlags(Map<?, ?> flag) {
            return Maps.transformValues(flag, this);
        }

        protected Iterable<?> transformSpecialFlags(Iterable<?> flag) {
            return Iterables.transform(flag, this);
        }

        protected BrooklynClassLoadingContext getLoader() {
            if (loader!=null) return loader;
            // TODO currently loader will non-null unless someone has messed with the rebind files,
            // but we'd like to get rid of it; ideally we'd have a reference to the entity.
            // for now, this is a slightly naff way to do it, if we have to set loader=null as a workaround
            Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            if (entity!=null) return CatalogUtils.getClassLoadingContext(entity);
            return JavaBrooklynClassLoadingContext.create(mgmt);
        }

        /**
         * Makes additional transformations to the given flag with the extra knowledge of the flag's management context.
         * @return The modified flag, or the flag unchanged.
         */
        protected Object transformSpecialFlags(Object flag) {
            if (flag instanceof EntitySpecConfiguration) {
                EntitySpecConfiguration specConfig = (EntitySpecConfiguration) flag;
                // TODO: This should called from BrooklynAssemblyTemplateInstantiator.configureEntityConfig
                // And have transformSpecialFlags(Object flag, ManagementContext mgmt) drill into the Object flag if it's a map or iterable?
                @SuppressWarnings("unchecked")
                Map<String, Object> resolvedConfig = (Map<String, Object>)transformSpecialFlags(specConfig.getSpecConfiguration());
                specConfig.setSpecConfiguration(resolvedConfig);
                return Factory.newInstance(getLoader(), specConfig.getSpecConfiguration()).resolveSpec();
            }
            if (flag instanceof ManagementContextInjectable) {
                log.debug("Injecting Brooklyn management context info object: {}", flag);
                ((ManagementContextInjectable) flag).injectManagementContext(loader.getManagementContext());
            }

            return flag;
        }
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> getChildren(Map<String, Object> attrs) {
        if (attrs==null) return null;
        return (List<Map<String, Object>>) attrs.get("brooklyn.children");
    }

}
