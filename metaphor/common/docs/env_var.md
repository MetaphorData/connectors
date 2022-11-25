# Environment Variable Substitution

You can specify variables using the form `${VAR_NAME}` in the config file, which will be automatically replaced by the value of the matching environment variable. For example, you can set the client secret in the config file using the `SECRET` environment variable:  

```yml
client_secret: ${SECRET}
```

The variables can be used anywhere in the config where a string value is expected. It can also be used as part of the string,

```yml
url: http://${HOST_NAME}/${PATH}
```
