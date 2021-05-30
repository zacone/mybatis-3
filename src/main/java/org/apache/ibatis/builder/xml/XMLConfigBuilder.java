/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * 读取xml文件构建核心配置实例configuration.
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  private boolean parsed;
  private final XPathParser parser;
  private String environment;
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    //通过reader读取xml文本到xml解析器
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    //通过stream读取xml文本到xml解析器
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  /**
   * 初始化xml配置的Builder类.
   *
   * @param parser      xml对象
   * @param environment 环境变量
   * @param props       mybatis动态配置
   */
  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    //初始化最核心的Configuration类
    super(new Configuration());
    //在当前线程错误上下文变量中标记
    ErrorContext.instance().resource("SQL Mapper Configuration");
    //保存mybatis动态配置
    this.configuration.setVariables(props);
    //设置成未解析
    this.parsed = false;
    //设置当前环境
    this.environment = environment;
    //保存xml解析器
    this.parser = parser;
  }

  /**
   * 将xml文件解析成configuration实例返回.
   *
   * @return Configuration类实例
   */
  public Configuration parse() {
    if (parsed) {
      //不允许重复解析
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    //标记已解析
    parsed = true;
    //将xml中的配置解析到成员变量configuration中
    parseConfiguration(parser.evalNode("/configuration"));
    //返回解析结果
    return configuration;
  }

  /**
   * 将xml配置全部解析到configuration对象中.
   *
   * <p>
   * 参考官方文档
   * <a href='https://mybatis.org/mybatis-3/configuration.html'>Configuration XML</a>.
   * <p>
   * xml配置分为:
   * <p>
   * properties
   * <p>
   * settings
   * <p>
   * typeAliases
   * <p>
   * typeHandlers
   * <p>
   * objectFactory
   * <p>
   * plugins
   * <p>
   * environments
   * <p>
   * databaseIdProvider
   * <p>
   * mappers
   *
   * @param root xml配置
   */
  private void parseConfiguration(XNode root) {
    try {
      // issue #117 read properties first
      //解析properties标签储存到configuration对象的成员变量variables中(resource外部文件,property子标签)
      propertiesElement(root.evalNode("properties"));
      //解析settings标签为properties对象并校验是否能够使用该设置项
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      //配置settings中的vfsImpl(虚拟文件系统实现类)
      loadCustomVfs(settings);
      //配置settings中的logImpl(日志系统实现类)
      loadCustomLogImpl(settings);
      //配置typeAliases(类别名)
      typeAliasesElement(root.evalNode("typeAliases"));
      //配置plugins(插件)
      pluginElement(root.evalNode("plugins"));
      //配置自定义objectFactory(用来实例化一些变量中储存的Class,比如resultType,resultMap,returnType.默认的实现类是 DefaultObjectFactory.java)
      objectFactoryElement(root.evalNode("objectFactory"));
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      //将settings配置储存到configuration对象
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      //解析environments根据环境配置对应的事务管理器,数据源
      environmentsElement(root.evalNode("environments"));
      //配置定制数据库类型(db2,mysql...)获取方法
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      //jdbcType转javaType的转换配置(package标签name包扫描,typeHandler子标签配置)
      typeHandlerElement(root.evalNode("typeHandlers"));
      //注册配置的Mapper到mapperRegistry
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    for (Object key : props.keySet()) {
      //因为xml文件中settings配置在configuration中都是单独的成员变量,所以需要校验Configuration类是否可以set这个变量名
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    String value = props.getProperty("vfsImpl");
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>) Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    //设置日志组件实现类
    configuration.setLogImpl(logImpl);
  }

  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          //使用包名扫描注册typeAliases
          String typeAliasPackage = child.getStringAttribute("name");
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
          //单独注册typeAliases
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            Class<?> clazz = Resources.classForName(type);
            if (alias == null) {
              typeAliasRegistry.registerAlias(clazz);
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        String interceptor = child.getStringAttribute("interceptor");
        Properties properties = child.getChildrenAsProperties();
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor().newInstance();
        interceptorInstance.setProperties(properties);
        //将xml配置中的插件保存到configuration实例的插件配置中(插件是个链式的拦截器)
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties properties = context.getChildrenAsProperties();
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(properties);
      configuration.setObjectFactory(factory);
    }
  }

  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setObjectWrapperFactory(factory);
    }
  }

  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setReflectorFactory(factory);
    }
  }

  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      //子标签自定义配置
      Properties defaults = context.getChildrenAsProperties();
      //外部文件配置(优先)
      String resource = context.getStringAttribute("resource");
      //外部URL配置(备选)
      String url = context.getStringAttribute("url");
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      //读取外部配置(优先resource,其次url)
      if (resource != null) {
        defaults.putAll(Resources.getResourceAsProperties(resource));
      } else if (url != null) {
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      Properties vars = configuration.getVariables();
      if (vars != null) {
        //configuration中已存在动态配置,将已存在配置添加当前读取的配置中
        defaults.putAll(vars);
      }
      parser.setVariables(defaults);
      //保存最新的动态配置到configuration对象的variables中
      configuration.setVariables(defaults);
    }
  }

  private void settingsElement(Properties props) {
    //是否启用自动映射(NONE,PARTIAL,FULL).嵌套结果条件下FULL启用,否则不为NONE则启用
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    //自动映射失败时的行为(`NONE`无任何行为,`WARNING打印`warn日志,`FAILING`抛出SqlSessionException异常)
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    //是否启用缓存
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    //设置代理模式的实现类(CGLIB,JAVASSIST或者自己实现`org.apache.ibatis.executor.loader.ProxyFactory`)
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    //设置是否启用懒加载
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    //懒加载时,是否懒加载类成员变量
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    //是否启用多结果集
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    //是否使用标签名代替列名
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    //是否使用`Jdbc3KeyGenerator`作为键生成器
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    //设置默认sql执行器(默认为SimpleExecutor)
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    //设置默认sql执行超时时间
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    //设置拉取数据时每次传输的数据条数
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    //默认的resultSetType.DEFAULT,FORWARD_ONLY,SCROLL_INSENSITIVE,SCROLL_SENSITIVE()
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    //值映射时是否下划线转驼峰后再匹配
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    //在存在嵌套结果时,是否允许分页(开启时,若rowBounds不为空且非默认值则会抛出异常)
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    //设置本地缓存作用范围(SESSION,STATEMENT)
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    //设置入参为null时参数的jdbc type
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    //设置代理时需要懒加载的方法
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    //在存在嵌套结果时,是否启用结果处理器安全检查(resultOrdered=false时不安全)
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    //设置默认sql脚本解析实现(默认为XMLLanguageDriver)
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    //设置enum类的typeHandler(此阶段只设置,不会注册到TypeHandlerRegistry,在getJdbcHandlerMap时存在enum字段时才会注册.默认实现为EnumTypeHandler)
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    //响应数据中字段为null时是否调用set方法,前提是字段不能为primitive(修复issues#377的bug)
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    //mapper类的方法入参未设置@Param的name时，是否使用实际字段名作为参数(仍未获取到字段名时字段名规则为"arg"+index)
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    //映射字段映射结果全部为null时,仍返回空结果对象还是返回null
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    //设置日志路径前缀
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    //设置Configuration实例的工厂类(有需要可以自己写一个类继承Configuration类拓展或重写一些功能，然后在此处通过工厂的`getConfiguration`方法替换configuration实例)
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
    //解析sql时是否格式化sql语句(清除多余空格)
    configuration.setShrinkWhitespacesInSql(booleanValueOf(props.getProperty("shrinkWhitespacesInSql"), false));
    //设置insert,delete,update,select,SelectKey之外的SqlSource提供类
    configuration.setDefaultSqlProviderType(resolveClass(props.getProperty("defaultSqlProviderType")));
  }

  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {
        String id = child.getStringAttribute("id");
        //自定义环境下使用该环境下的事务与数据源
        if (isSpecifiedEnvironment(id)) {
          //获取事务工厂实例
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          //获取数据源工厂实例
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          //从工厂类获取数据源实例
          DataSource dataSource = dsFactory.getDataSource();
          //配置环境
          Environment.Builder environmentBuilder = new Environment.Builder(id)
            .transactionFactory(txFactory)
            .dataSource(dataSource);
          configuration.setEnvironment(environmentBuilder.build());
          break;
        }
      }
    }
  }

  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
        //DB_VENDOR对应的实现类是VendorDatabaseIdProvider，获取`java.sql.DatabaseMetaData`的`getDatabaseProductName`方法返回值与子标签的properties匹配确定databaseId值
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor().newInstance();
      //将xml配置中databaseIdProvider配置保存到DatabaseIdProvider的实现类中
      databaseIdProvider.setProperties(properties);
    }
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      //如果配置了DatabaseIdProvider,则调用其实现类确定当前环境的数据库类型(databaseId)并保存到configuration
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      //将xml配置中的事务配置保存到工厂实例
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      //将xml配置中的数据库配置保存到工厂实例
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          //扫描包路径注册typeHandlers
          String typeHandlerPackage = child.getStringAttribute("name");
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          //单个注册typeHandlers
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          //扫描包添加mapper
          String mapperPackage = child.getStringAttribute("name");
          configuration.addMappers(mapperPackage);
        } else {
          //单个添加mapper(3种配置方式resource,url,class)
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");
          if (resource != null && url == null && mapperClass == null) {
            ErrorContext.instance().resource(resource);
            //读取resource配置的xml文件路径
            try (InputStream inputStream = Resources.getResourceAsStream(resource)) {
              XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
              mapperParser.parse();
            }
          } else if (resource == null && url != null && mapperClass == null) {
            ErrorContext.instance().resource(url);
            //读取url配置的xml文件路径
            try (InputStream inputStream = Resources.getUrlAsStream(url)) {
              XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
              mapperParser.parse();
            }
          } else if (resource == null && url == null && mapperClass != null) {
            //配置MapperInterface
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    }
    if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    }
    return environment.equals(id);
  }

}
