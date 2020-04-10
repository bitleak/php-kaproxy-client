# php-kaproxy-client

Impl php client for Kaproxy(HTTP Proxy For Kafka)

## Install 

```
> git clone git@gitlab.meitu.com:platform/php-kaproxy-client.git
> cd php-kaproxy-client 
> composer install
```

## Example

```php
require 'vendor/autoload.php';

use Kaproxy\Client;

$addr = "http://127.0.0.1:8080";
$token = "60009dfa81c54cf99ac843b8b3bc0db1";

$cli = new Client($addr, $token);
var_dump($cli->Produce("test-topic", "test_key", "test_value"));
var_dump($cli->Consume("test-group", "test-topic"));
$cli->Close();
```

## Test

```
> make test
```
