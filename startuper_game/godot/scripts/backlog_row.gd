extends VBoxContainer
class_name BacklogRow

## Принимает drop из слота — возвращает карточку в бэклог.

var main_ref: Control = null

func set_main(p_main: Control) -> void:
	main_ref = p_main
	mouse_filter = Control.MOUSE_FILTER_STOP

func _can_drop_data(_at_position: Vector2, data: Variant) -> bool:
	if main_ref == null or not main_ref.has_method("can_return_task_to_backlog"):
		return false
	return main_ref.can_return_task_to_backlog(data)

func _drop_data(_at_position: Vector2, data: Variant) -> void:
	if main_ref == null or not main_ref.has_method("return_task_to_backlog"):
		return
	main_ref.return_task_to_backlog(data)
