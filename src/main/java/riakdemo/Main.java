package riakdemo;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import riakdemo.backend.BackendException;
import riakdemo.backend.CounterUpdate;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class Main {
    public static void main(String[] args) throws BackendException {
        Integer contactPointPort = 8087;
        String[] contactPoints = new String[]{"150.254.32.158", "150.254.32.161"};

        String bucketType = "r1w1";
        String namespaceName = "kappa";
        String clientName = "Darek";

        RiakClient client;

        try {
            client = RiakClient.newClient(contactPointPort, contactPoints);
        } catch (UnknownHostException e) {
            throw new BackendException("Kappa");
        }

        Namespace namespace = new Namespace(bucketType, namespaceName);

        Function<Integer, Runnable> code = (threadNum) -> () -> {
            Integer localCounter = 0;
            String key = clientName + threadNum + "/" + UUID.randomUUID().toString();
            Location location = new Location(namespace, key);

            while (true) {
                try {
                    UpdateValue update = new UpdateValue
                            .Builder(location)
                            .withStoreOption(StoreValue.Option.RETURN_BODY, true)
                            .withUpdate(new CounterUpdate())
                            .build();

                    UpdateValue.Response response = client.execute(update, 1000, TimeUnit.MILLISECONDS);
                    Integer counter = response.getValue(Integer.class);
                    localCounter++;

                    System.out.println(
                            String.format(
                                    "My name is: %s, Counter: %d, local counter: %d",
                                    key,
                                    counter,
                                    localCounter
                            )
                    );
                } catch (ExecutionException e) {
                    System.out.println("Execution exception - something went wrong for " + key);
                } catch (TimeoutException e) {
                    System.out.println("Timeout exception - timeout for " + key);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception for " + key);
                } catch (Exception e) {
                    System.out.println("Random exception");
                    e.printStackTrace();
                }
            }
        };

        try {
            int n = 10;
            Thread[] threads = new Thread[n];

            for (int i = 0; i < n; i++) {
                threads[i] = new Thread(code.apply(i));
                threads[i].start();
            }

            for (int i = 0; i < n; i++) {
                threads[i].join();
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception when loading.");
        } catch (Exception e) {
            System.out.println("Random exception.");
            e.printStackTrace();
        }

        System.exit(0);
    }
}
