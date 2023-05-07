package study.querydsl.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class UserDto {

    private String name;
    private int userage;

    public UserDto(String name, int userage) {
        this.name = name;
        this.userage = userage;
    }
}
