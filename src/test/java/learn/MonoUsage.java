package learn;

import org.testng.annotations.Test;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class MonoUsage {

  public Disposable testRepeat(){
    return Mono.fromCallable(()->System.currentTimeMillis())
        .repeat()
//        .publishOn(Schedulers.single())
//        .log("foo.bar")
        .subscribe(System.out::println);
  }

  @Test
  public void testWithWarm(){
    Disposable justUseAsWarm = testRepeat();

    justUseAsWarm.dispose();
    if(justUseAsWarm.isDisposed()){
      testRepeat();
    }
  }
}
