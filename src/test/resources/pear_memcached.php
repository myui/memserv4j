<?php
    $memc = new Memcached();

    if (!$memc) {
        echo("failed to instantiate memcached object\n");
        exit(1);
    }

    $memc->setOption(Memcached::OPT_BINARY_PROTOCOL, true);
    //$memc->setOption(Memcached::OPT_CONNECT_TIMEOUT, 10000);
    //$memc->setOption(Memcached::OPT_POLL_TIMEOUT, 10000);
    //$memc->setOption(Memcached::OPT_NO_BLOCK, true);
    $memc->addServer('192.168.142.144', 11211);

    if (!$memc->set('some_key', 'this is a value')) {
      echo("failed to set an item\n");
      exit(1);
    }
    echo "set ",$memc->getResultMessage(),"\n";

    echo($memc->get('some_key') . "\n");
    echo "get ",$memc->getResultMessage(),"\n";
    echo($memc->get('some_key') . "\n");
    echo "get ",$memc->getResultMessage(),"\n";
    echo($memc->get('some_key') . "\n");
    echo "get ",$memc->getResultMessage(),"\n";
?>