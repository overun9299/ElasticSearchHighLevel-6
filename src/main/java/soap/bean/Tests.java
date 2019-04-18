package soap.bean;

/**
 * @ClassName: Tests
 * @Description:
 * @author: 薏米滴答-西安-zhangPY
 * @version: V1.0
 * @date: 2019/4/18 10:15
 * @Copyright: 2019 www.yimidida.com Inc. All rights reserved.
 */
public class Tests {

    private Long id;

    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Tests [id=" + id + ", name=" + name + "]";
    }

}
