package bg.tid.iec104.db.service;

import java.util.List;
import java.util.Optional;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import bg.tid.iec104.db.entity.Command;
import bg.tid.iec104.db.repository.CommandRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@AllArgsConstructor
public class CommandService {

	private final CommandRepository commandRepository;
	private final ModelMapper mm = new ModelMapper();

	public List<Command> getAllCommands() {
		log.debug("getAllCommands");

		return commandRepository.findAll();
	}

	public Optional<Command> getCommandById(long id) {
		log.debug("getCommandById");

		return commandRepository.findById(id);
	}
	
	public Command createCommand(Command command) {
		log.debug("createCommand");

		try {
			Command newCommand = commandRepository.save(command);
			return newCommand;

		} catch (Exception e) {
			return null;
		}
	}

	public Command updateCommand(Command newCommand) {
		log.debug("updateCommand");

		try {
			Command oldCommand = commandRepository.findById(newCommand.getId()).get();
			if (oldCommand != null) {
				mm.map(newCommand, oldCommand);
				Command command = commandRepository.save(oldCommand);
				return command;
			} else {
				return null;
			}

		} catch (Exception e) {
			return null;
		}
	}

	public Boolean deleteCommand(Long id) {
		log.debug("deleteCommand");

		try {
			commandRepository.deleteById(id);
			return true;

		} catch (Exception e) {
			return false;
		}
	}

	public Optional<Command> getCommandByName(String topic) {
		return commandRepository.findByTopic(topic);

	}
}
