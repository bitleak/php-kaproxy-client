<?php

namespace Kaproxy;

class Client {
    private $addr; 
    private $namespace;
    private $token;
    private $url;


    /**
     * constructor
     *
     * @var String $addr        server address, eg: "127.0.0.1:8080"
     * @var String $token       secret to access the group/topic
     */
    public function __construct($addr, $token) {
        $this->addr = $addr;
        $this->token = $token;
        $this->ch = curl_init();
    }

    /**
     * Publish a job to kaproxy 
     *
     * @var String $topic       topic name 
     * @var String $key         message key
     * @var String $value       message value 
     * @var String $partitioner hash|random, default is hash 
     * @var int    $timeout     curl timeout, default is 1000ms
     * @var bool   $replicate   replicate to other idc or not
     */
    public function Produce($topic, $key, $value, $partitioner = 'hash', $timeout = 1000, $replicate = true) {
        if (empty($topic) || empty($value)) {
            throw new \Exception("topic or value can't be empty");
        }
        if ($partitioner != "random" && empty($key)) {
            throw new \Exception("the key can't be empty while the partitioner is hash");
        }
        if ($partitioner != "random" && $partitioner != "hash") {
            $partitioner = "hash";
        }
        $data = array(
            "key" => $key,
            "value" => $value,
            "partitioner" => $partitioner,
            "replicate" => $replicate ? "yes":"no"
        );
        $query = array('token'=>$this->token);
        $response = $this->doRequest("topic/".$topic, "POST", http_build_query($query), $data, $timeout);
        $msg = json_decode($response['body'], true); 
        if (!empty($msg['error'])) {
            throw new \Exception("failed to produce while encounter error: ".$msg['error']);
        }
        // FIX bug, the produce response before kaproxy 0.4 with initials in capitals.
        return array_change_key_case($msg, CASE_LOWER);
    }

    /*
     * Consume message from kaproxy
     * @var String $group       consumer group
     * @var Int    $topic       topic name
     * @var Int    $blockingTimeout client blocking wait for new message(millisecond)
     */
    public function Consume($group, $topic, $blockingTimeout = 3000) {
        if (empty($group) || empty($topic)) {
            throw new \Exception("group or topic name can't be empty");
        }
        if ($blockingTimeout < 1000) {
            throw new \Exception("blocking timeout should be > 1000ms");
        }
        // use blocking timeout as http timeout, and blocking timeout should be smaller,
        // while the RTT(round trip time) may take some time.
        $timeout = $blockingTimeout*3/2;
        $query = array('token'=>$this->token, 'timeout'=>$blockingTimeout);
        $relativePath = "group/$group/topic/$topic";
        $response = $this->doRequest($relativePath, "GET", http_build_query($query), "", $timeout);
        if ($response['code'] == 204) {
            return NULL;
        }
        $msg = json_decode($response['body'], true); 
        if (!empty($msg['error'])) {
            if ($msg['error'] == 'No message in broker' || $msg['error'] == 'no message in broker') {
                return NULL;
            }
            throw new \Exception("failed to consume while encounter error: ".$msg['error']);
        }
        if ($msg['encoding'] == "base64") {
            $msg['value'] = base64_decode($msg['value']);
        }
        return $msg;
    }

    /*
     * Close the http connection
     */
    public function Close() {
        curl_close($this->ch);
        $this->ch = NULL;
    }

    private function doRequest($relativePath, $method, $query='', $data='', $timeout = 300000) {
        if ($this->ch == NULL) {
            $this->ch = curl_init();
        }
        $headers= array(
            "X-Token:".$this->token
        );
        $url = $this->addr.'/'.$relativePath;
        if (!empty($query)) {
            $url = $url.'?'.$query;
        }
        $ch = $this->ch;
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        // connect timeout 1500ms
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT_MS, 1500);
        curl_setopt($ch, CURLOPT_NOSIGNAL, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT_MS, $timeout);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        if (!empty($data)) {
            if (is_string($data)) {
                curl_setopt($ch, CURLOPT_POSTFIELDS,$data);
            } else {
                curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query($data));
            }
        }
        $body = curl_exec($ch);
        $errno = curl_errno($ch);
        if ($errno != 0 && $errno != CURLE_HTTP_NOT_FOUND) {
            $this->Close();
            throw new \Exception("failed to curl while error:".curl_strerror($errno));
        }
        $res = array(
            'code' => curl_getinfo($ch, CURLINFO_HTTP_CODE),
            'body' => $body
        );
        return $res;
    }
}
